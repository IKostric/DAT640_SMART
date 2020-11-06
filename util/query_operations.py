#%%
if os.getcwd().endswith('util'):
    os.chdir('../')

from util.io import load_dict_from_json, save_dict_to_json
from smart_dataset.evaluation.dbpedia.evaluate import load_type_hierarchy


#%%
def remove_non_existing_types(filename, type_hierarchy):
    s = load_dict_from_json(filename)
    i = 0
    for q in s:
        if q['category'] == 'resource':
            q['type'] = [t for t in q['type'] if t in type_hierarchy]
            i += 1

    print(f'Removed "dbo:Location" from {i} instances.')
    save_dict_to_json(s, filename)


def remove_empty_queries(filename='train_set_fixed.json'):
    s = load_dict_from_json(filename)
    num_queries = len(s)
    s = [q for q in s if q['question']]
    print(f'Removed {num_queries-len(s)} from the dataset')
    save_dict_to_json(s, filename)


#%%
if __name__ == "__main__":

    type_hierarchy, max_depth = load_type_hierarchy(
        './smart_dataset/evaluation/dbpedia/dbpedia_types.tsv')
    filenames = ('train_set_fixed.json', 'test_set_fixed.json',
                 'validation_set_fixed.json')
    for fname in filenames:
        remove_empty_queries(fname)
        remove_non_existing_types(fname, type_hierarchy)

# %%
