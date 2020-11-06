#%%
import os
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk

if os.getcwd().endswith('util'):
    os.chdir('../')

# fix this
from util.parse_dbpedia import get_TC_documents, get_EC_documents
from util.io import load_dict_from_json, save_dict_to_json


#%%
class ES:

    def __init__(self, model='EC', similarity='BM25'):
        self.model = model
        self.similarity = similarity

        self._settings = self.get_model_settings()
        self._index_name = f'{model}_{similarity}'.lower()

        if similarity == 'LM':
            self._settings['settings'] = self.get_LM_settings()

        self.es = Elasticsearch(timeout=60)
        print(self.es.info())

    def get_index(self):
        return self._index_name

    def get_LM_settings(self):
        return {"index": {"similarity": {"default": {"type": "LMDirichlet"}}}}

    def get_model_settings(self):
        properties = {
            'body': {
                'type': 'text',
                'term_vector': 'yes',
                'analyzer': 'english'
            }
        }
        # if self.model == 'EC':
        #     properties['type'] = {
        #         'type': 'text',
        #         'term_vector': 'yes',
        #         'analyzer': 'english'
        #     },

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

    def reindex(self, doc_body=None):
        if self.model == 'EC':
            chunk_size = 5000
            documents = get_EC_documents(doc_body or 'short')
        else:
            chunk_size = 1
            documents = get_TC_documents(doc_body or 'anchor')
        self.reset_index()
        for success, info in parallel_bulk(self.es,
                                           self.data_from_generator(documents),
                                           thread_count=12,
                                           chunk_size=chunk_size,
                                           queue_size=6):
            if not success:
                print('A document failed:', info)

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

    def baseline_retrieval(self, queries, k=100):
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

        res = self.baseline_retrieval(queries)
        save_dict_to_json(res, fname)
        return res


if __name__ == "__main__":
    config = ES('EC', 'BM25')

# %%
