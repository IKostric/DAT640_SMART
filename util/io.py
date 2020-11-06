import os
import json


def get_data_path(filename, dbpedia=False):
    return os.path.join(os.getcwd(), 'data', 'dbpedia' if dbpedia else '',
                        filename)


def save_dict_to_json(doc, filename):
    with open(get_data_path(filename), 'w') as f:
        json.dump(doc, f)


def load_dict_from_json(filename):
    try:
        with open(get_data_path(filename), 'r') as f:
            doc = json.load(f)
        return doc
    except:
        print('File not found.')
        return None