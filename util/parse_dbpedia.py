#%%
import os, json, string
from collections import defaultdict
from nltk.corpus import stopwords

if os.getcwd().endswith('util'):
    os.chdir('../')

from util.io import get_data_path, load_dict_from_json, save_dict_to_json

STOPWORDS = stopwords.words('english')


#%% PREPROCESS TEXT
def process(text):
    text = text.replace('\'', '')
    text = ''.join(ch if ch not in string.punctuation else ' ' for ch in text)
    text = ' '.join(word for word in text.split(' ') if word not in STOPWORDS)
    return text


#%% PARSE TTL
def resolve_uri(uri):
    if uri.startswith('http://dbpedia.org/resource/'):
        uri = uri[len('http://dbpedia.org/resource/'):]
    else:
        uri = uri.split('/')[-1].replace('_', ' ')
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


#%% ONTOLOGY
def get_ontology(force=False):
    fname = 'ontology.json'
    if not force:
        ontology = load_dict_from_json(fname)
        if ontology:
            return ontology

    print('Creating new ontology file.')
    ontology = defaultdict(dict)
    with open(get_data_path('dbpedia_2016-10.nt', True), 'r',
              encoding='UTF-8') as f:
        for line in f:
            if line.startswith('#'):
                continue

            s, p, o = parse_ttl_line(line)
            if (p != 'http://www.w3.org/2000/01/rdf-schema#subClassOf'):
                continue

            if not ('owl#Thing' in o
                    or o.startswith('http://dbpedia.org/ontology/')):
                continue

            subj = resolve_uri(s)
            if not subj:
                continue

            subj = 'dbo:' + subj
            obj = resolve_uri(o)
            obj = obj if obj == 'owl#Thing' else 'dbo:' + obj
            if subj in ontology:
                ontology[subj]['parents'].append(obj)
            else:
                ontology[subj]['parents'] = [obj]

    for entity in ontology:
        path = list(set(get_path(ontology, entity)))
        ontology[entity]['path'] = path
        ontology[entity]['num_ancestors'] = len(path)

    print(f'There are {len(ontology)} types')
    save_dict_to_json(ontology, fname)
    return ontology


def get_path(tree, entity_type):
    path = [] if entity_type == 'owl#Thing' else [entity_type]
    if entity_type not in tree:
        return path

    for et in tree[entity_type]['parents']:
        path.extend(get_path(tree, et))

    return path


#%% ENTITY - TYPE
def get_instance_types(filename='instance_types_en.ttl',
                       documents=defaultdict(list)):
    with open(get_data_path(filename, dbpedia=True), 'r',
              encoding='UTF-8') as f:  #, 'rb') as f:
        for line in f:
            if line.startswith('#'):
                continue

            s, p, o = parse_ttl_line(line)
            if (not o.startswith('http://dbpedia.org/ontology/')
               ) or 'Wikidata:' in o:
                continue

            obj = resolve_uri(o)
            if not obj or obj == 'owl#Thing':
                continue

            subj = resolve_uri(s)
            if subj:
                documents[subj].append('dbo:' + obj)

    print('Num entities with types: ', len(documents))
    return documents


def get_all_instance_types(transitive=False, force=False):
    fname = f'instance_types{"_all" if transitive else ""}.json'
    if not force:
        instance_types = load_dict_from_json(fname)
        if instance_types:
            return instance_types

    print("Creating new instance types mapping...")
    instance_type_filenames = [
        'instance_types_en.ttl', 'instance_types_sdtyped_dbo_en.ttl'
    ]
    if transitive:
        instance_type_filenames.append('instance_types_transitive_en.ttl')

    instance_types = defaultdict(list)
    for filename in instance_type_filenames:
        instance_types = get_instance_types(filename, instance_types)

    if transitive:
        ontology = get_ontology()

    for entity in instance_types:
        if transitive:
            types = []
            for t in instance_types[entity]:
                types.extend(ontology.get(t, {}).get('path', []))
            instance_types[entity] = list(set(types))
        else:
            instance_types[entity] = list(set(instance_types[entity]))

    save_dict_to_json(instance_types, fname)
    return instance_types


def get_type_entity(ancestors=False, force=False):
    fname = f'type_entity{"_all" if ancestors else ""}.json'
    if not force:
        documents = load_dict_from_json(fname)
        if documents:
            return documents

    print('Creating new type_entity file.')
    entities_with_types = get_all_instance_types(ancestors)
    documents = defaultdict(list)
    for entity, types in entities_with_types.items():
        for t in types:
            documents[t].append(entity)

    print('Done. Num types: ', len(documents))
    save_dict_to_json(documents, fname)
    return documents


#%% CREATE BODY FOR DOCUMENTS
def get_entity_data(filename='short_abstracts_en.ttl'):
    documents = defaultdict(list)
    with open(get_data_path(filename, dbpedia=True), 'r',
              encoding='UTF-8') as f:
        for line in f:
            if line.startswith('#'):
                continue

            s, p, o = parse_ttl_line(line)
            subj = resolve_uri(s)
            if not subj:
                continue

            data = process(o)
            documents[subj].append(data)

    # This is mostly done because of anchor text that is often same
    for entity in documents:
        documents[entity] = ' '.join(set(documents[entity]))
    print('Num entities with data: ', len(documents))
    return documents


def get_document_bodies(keyword=None, force=False):
    fname = 'document_bodies{}.json'.format('_' + keyword if keyword else '')
    if not force:
        document_bodies = load_dict_from_json(fname)
        if document_bodies:
            return document_bodies

    print('Creating new entity bodies.')
    entities_with_types = get_all_instance_types()

    long_abstracts = get_entity_data(
        'long_abstracts_en.ttl') if not keyword or keyword == 'long' else {}
    short_abstracts = get_entity_data(
        'short_abstracts_en.ttl') if not keyword or keyword == 'short' else {}
    anchor_texts = get_entity_data(
        'anchor_text_en.ttl') if not keyword or keyword == 'anchor' else {}

    document_bodies = defaultdict(str)
    for entity in entities_with_types:
        if (not keyword or keyword == 'short') and entity in short_abstracts:
            document_bodies[entity] = short_abstracts[entity]
        elif (not keyword or keyword == 'long') and entity in long_abstracts:
            document_bodies[entity] = long_abstracts[entity]

        if (not keyword or keyword == 'anchor') and entity in anchor_texts:
            document_bodies[entity] += anchor_texts[entity]

    save_dict_to_json(document_bodies, fname)
    print(f'Created {len(document_bodies)} document bodies.')
    return document_bodies


def get_EC_documents(doc_body='short', force=False):
    filename = 'document_EC{}.json'.format('_' + doc_body if doc_body else '')
    if not force:
        document = load_dict_from_json(filename)
        if document:
            return document

    print('Creating new document.')
    bodies = get_document_bodies(doc_body)
    # types = get_all_instance_types(transitive=True)

    document = defaultdict(dict)
    for entity, body in bodies.items():
        document[entity]['body'] = body
        # document[entity]['type'] = ' '.join(types[entity])

    save_dict_to_json(document, filename)
    return document


def get_TC_documents(doc_body='anchor', ancestors=False, force=False):
    filename = 'document_TC{}{}.json'.format('_' + doc_body if doc_body else '',
                                             '_all' if ancestors else '')
    if not force:
        document = load_dict_from_json(filename)
        if document:
            return document

    print('Creating new document.')
    bodies = get_document_bodies(doc_body)
    type_entities = get_type_entity(ancestors)
    num_types = len(type_entities)

    document = defaultdict(dict)
    for i, (t, entities) in enumerate(type_entities.items()):
        print(
            f'Processing {i+1}/{num_types} type with {len(entities)} entities')
        document[t]['body'] = ' '.join(
            bodies.get(entity, '') for entity in entities)

    save_dict_to_json(document, filename)
    return document


def get_type_weights(force=False):
    fname = 'type_weight.json'
    if not force:
        weight_doc = load_dict_from_json(fname)
        if weight_doc:
            return weight_doc

    print('Making new type_weight file.')
    types = get_all_instance_types(True)
    weight_doc = defaultdict(int)
    for _, types in types.items():
        for t in types:
            weight_doc[t] += 1

    save_dict_to_json(weight_doc, fname)
    return weight_doc


# %%
