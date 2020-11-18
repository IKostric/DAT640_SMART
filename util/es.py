#%%
import os
from collections import defaultdict
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk

from util.parse_dbpedia import get_TC_documents, get_EC_documents, get_type_weights, get_all_instance_types
from util.io import load_dict_from_json, save_dict_to_json


class ES:

    def __init__(self, model='EC', similarity='BM25'):
        self.model = model
        self.similarity = similarity

        self._settings = self.get_model_settings()
        self._index_name = f'{model}_{similarity}'.lower()

        self._settings['settings'] = getattr(
            self, f'get_{similarity.lower()}_settings')()

        self.es = Elasticsearch(timeout=120)
        #print(self.es.info())

    def get_index(self):
        return self._index_name

    def get_lm_settings(self):
        return {"similarity": {"default": {"type": "LMDirichlet"}}}

    def get_bm25_settings(self):
        return {"similarity": {"default": {"type": "BM25"}}}

    def get_custom_settings(self):
        return {
            "similarity": {
                "default": {
                    "type": "LMDirichlet"
                },
                "custom_LMDirichlet": {
                    "type": "scripted",
                    "script": {
                        "source":
                            """
                            double freq = doc.freq;
                            double PtC = (term.totalTermFreq+1.0)/(field.sumTotalTermFreq+1.0);
                            double tw = Math.log(1.0 + (freq/(2000.0*PtC)));
                            double norm = Math.log(2000.0 / (doc.length + 2000.0));
                            return Math.max((tw + norm), 0.0);
                            """
                    }
                }
            }
        }

    def get_model_settings(self):
        properties = {
            'body': {
                'type': 'text',
                'term_vector': 'yes',
                'analyzer': 'english',
                'similarity':
                    'default'  #'custom_lm' if self.similarity == 'Custom' else 'default'
            }
        }
        if self.similarity == 'Custom':
            properties['weight'] = {
                'type': 'float',
                'index': False,
                "store": True
            }

        return {'mappings': {'properties': properties}}

    def reset_index(self):
        if self.es.indices.exists(self._index_name):
            self.es.indices.delete(self._index_name)

        self.es.indices.create(self._index_name, self._settings)

    def data_from_generator(self, doc):
        num_docs = len(doc) // 10
        for i, (doc_id, body) in enumerate(doc.items()):
            yield {'_index': self._index_name, '_id': doc_id, '_source': body}
            if i % num_docs == 0:
                print('{}% done'.format((i // num_docs) * 10))

    def _index_EC(self, documents):
        for success, info in parallel_bulk(self.es,
                                           self.data_from_generator(documents),
                                           thread_count=12,
                                           chunk_size=5000,
                                           max_chunk_bytes=104857600,
                                           queue_size=6):
            if not success:
                print('A document failed:', info)

    def _index_TC(self, documents):
        num_docs = len(documents) // 10
        for i, (did, body) in enumerate(documents.items()):
            self.es.index(self._index_name, body=body, id=did)
            if i % num_docs == 0:
                print('{}% done'.format((i // num_docs) * 10))

    def reindex(self, doc_body='short', ancestors=False):
        print('Indexing model {} - {}'.format(self.model, self.similarity))
        self.reset_index()
        if self.model == 'EC':
            documents = get_EC_documents(doc_body)
            self._index_EC(documents)
        else:
            documents = get_TC_documents(doc_body, ancestors)
            if self.similarity == 'Custom':
                weights = get_type_weights()
                for t in documents:
                    documents[t]['weight'] = weights.get(t, 1)
            self._index_TC(documents)
            # self._index_TC({k: v for k, v in list(documents.items())[:20]})

    def analyze_query(self, query, field='body'):
        """Analyzes a query with respect to the relevant index.
        
        Arguments:
            query: String of query terms.
            field: The field with respect to which the query is analyzed.
        
        Returns:
            A list of query terms that exist in the specified field among the documents in the index. 
        """
        tokens = self.es.indices.analyze(index=self._index_name,
                                         body={'text': query})['tokens']
        query_terms = []
        for t in sorted(tokens, key=lambda x: x['position']):
            ## Use a boolean query to find at least one document that contains the term.
            hits = self.es.search(index=self._index_name,
                                  body={
                                      'query': {
                                          'match': {
                                              field: t['token']
                                          }
                                      }
                                  },
                                  _source=False,
                                  size=1).get('hits', {}).get('hits', {})
            doc_id = hits[0]['_id'] if len(hits) > 0 else None
            if doc_id is None:
                continue
            query_terms.append(t['token'])
        return query_terms

    def baseline_EC_retrieval(self, queries, k=100):
        """Performs baseline retrival on index.
        """
        ids, body = [], []
        for query in queries:
            if query['category'] != 'resource':
                continue

            q = self.analyze_query(query['question'])
            if not q:
                continue

            ids.append(query['id'])
            body.append({})
            body.append({
                'query': {
                    'match': {
                        'body': ' '.join(q)
                    }
                },
                '_source': False,
                'size': k
            })
        res = self.es.msearch(index=self._index_name, body=body)['responses']

        return {
            qid: [(doc['_id'], doc['_score']) for doc in hits['hits']['hits']
                 ] for qid, hits in zip(ids, res)
        }

    def baseline_TC_retrieval(self, queries, k=100):
        """Performs baseline retrival on index.
        """
        results = {}
        for query in queries:
            if query['category'] != 'resource':
                continue

            q = self.analyze_query(query['question'])
            if not q:
                continue

            body = []
            for term in q:
                body.append({})
                body.append({
                    'query': {
                        'match': {
                            'body': term
                        }
                    },
                    '_source': False
                })
            res = self.es.msearch(index=self._index_name,
                                  body=body)['responses']

            scores = defaultdict(int)
            for hits in res:
                for doc in hits['hits']['hits']:
                    scores[doc['_id']] += doc['_score']

            results[query['id']] = sorted(scores.items(),
                                          key=lambda x: x[1],
                                          reverse=True)[:k]

        return results

    def load_baseline_results(self, dataset='train', force=False):
        fname = f'top100_{self.model}_{self.similarity}_{dataset}'
        if not force:
            results = load_dict_from_json(fname)
            if results:
                return results

        print('Retrieving from index.')
        queries = load_dict_from_json(f'{dataset}_set_fixed.json')
        if not queries:
            print('Cannot find the dataset.')
            return None

        res = getattr(self, f'baseline_{self.model}_retrieval')(queries)
        save_dict_to_json(res, fname)
        return res

    def get_baseline_EC_scores(self, results, k=100):
        """Aggregates scores from EC index and return ranked types

        Args:
            results (dict): baseline entity retrieval
            k (int, optional): Number of documents to aggregate over. Defaults to 100.

        Returns:
            dict: Type scores
        """
        type_weights = get_type_weights()
        entity_types = get_all_instance_types(True)
        system_output = {}
        for qid, res in results.items():
            scores = defaultdict(int)
            for entity, score in res[:k]:
                for t in entity_types[entity]:
                    scores[t] += score / type_weights[t]

            system_output[qid] = sorted(scores.items(),
                                        key=lambda x: x[1],
                                        reverse=True)
        return system_output

    def get_baseline_TC_scores(self, results, k=None):
        return results

    def generate_baseline_scores(self, dataset='train', k=100, force=False):
        raw_results = self.load_baseline_results(dataset, force)
        return getattr(self, f'get_baseline_{self.model}_scores')(raw_results,
                                                                  k)


if __name__ == "__main__":
    pass
    # ES('EC', 'BM25').reindex(None)
    # ES('TC', 'BM25').reindex(None, True)
