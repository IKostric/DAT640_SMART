#%%
import os
from collections import defaultdict


def resolve_uri(uri):
    uri = uri.split('/')[-1].replace('_', ' ')
    # if uri.startswith('Category:'):
    #     uri = uri[len('Category:'):]
    return uri


def parse_ttl_line(line):
    subject_index_end = line.index(' ')
    subject = line[1:subject_index_end - 1]
    line = line[subject_index_end + 1:]

    predicate_index_end = line.index(' ')
    predicate = line[1:predicate_index_end - 1]
    line = line[predicate_index_end + 1:]

    if line.startswith('<'):
        # uri
        obj = line.split(' ')[0][1:-1]

    elif line.startswith('"'):
        # literal
        obj = '"'.join(line.split('"')[1:-1])

    return subject, predicate, obj


def get_entity_types(ontology, filename='./data/dbpedia/instance_types_en.ttl'):
    documents = {}
    with open(filename, 'r', encoding='ISO-8859-1') as f:  #, 'rb') as f:
        for line in f:
            if line.startswith('#'):
                continue

            s, p, o = parse_ttl_line(line)
            if not o.startswith('http://dbpedia.org/ontology/'):
                continue

            obj = resolve_uri(o)
            if obj == 'owl#Thing':
                continue

            subj = resolve_uri(s)
            if subj:
                if subj in documents:
                    for ancestor in get_ancestors(ontology, obj):
                        if ancestor not in documents[subj]['types']:
                            documents[subj]['types'] += ' ' + ancestor
                else:
                    documents[subj] = {
                        'body': subj,
                        'types': ' '.join(set(get_ancestors(ontology, obj)))
                    }

    print('Num processed docs: ', len(documents))
    return documents


def get_type_from_entities(entity_documents):
    documents = {}
    for entity, body in entity_documents.items():
        for t in body['types'].split():
            if t in documents:
                documents[t]['entities'] += ' ' + entity
            else:
                documents[t] = {'body': t, 'entities': entity}

    print('Num types: ', len(documents))
    return documents


def get_ontology_tree(filename='data/dbpedia/dbpedia_2016-10.nt'):
    tree = defaultdict(list)
    with open(os.path.join(os.getcwd(), filename), 'r', encoding='UTF-8') as f:
        for line in f:
            if line.startswith('#'):
                continue

            s, p, o = parse_ttl_line(line)
            if not (p == 'http://www.w3.org/2000/01/rdf-schema#subClassOf'):
                continue

            if not o.startswith('http://dbpedia.org/ontology/'):
                continue

            obj = resolve_uri(o)
            if obj == 'owl#Thing':
                continue

            subj = resolve_uri(s)
            tree[subj].append(obj)

    return tree


def get_ancestors(tree, entity_type):
    ancestors = [entity_type]
    if entity_type not in tree:
        return ancestors

    for et in tree[entity_type]:
        ancestors.extend(get_ancestors(tree, et))

    return ancestors


if __name__ == "__main__":
    ontology = get_ontology_tree()
    doc_entity = get_entity_types(ontology)
    doc_type = get_type_from_entities(doc_entity)
# %%
