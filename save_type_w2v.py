import numpy as np
import json
import pickle
import os

from nltk.tokenize import RegexpTokenizer

import gensim.downloader as api
from gensim.models import Word2Vec

import spacy
from spacy.lang.en import English
from spacy.lang.en.stop_words import STOP_WORDS

from sklearn.metrics.pairwise import cosine_similarity

import time

import numba as nb
from concurrent.futures import ThreadPoolExecutor

def split_doc(doc,s_len=800000):
    '''
    Splits docs at the closes following blank space to the specified length.
    '''
    splits = [0]
    while True:
        s = splits[-1] + s_len
        while True:
            if s >= len(doc):
                splits.append(len(doc))
                return splits
            if doc[s]==' ':
                break
            else:
                s+=1
        splits.append(s)
    return splits

def check_token(mod,token):
    '''
    Tests if word is stopword, punctuation or if word does not exist in vocabulary.
    '''
    try:
        return mod.vocab[token].is_stop or mod.vocab[token].is_punct
    except:
        return True

#loading data
with open(os.path.join('data','document_TC_short.json'), 'r') as f:
    t_docs = json.load(f)

with open(os.path.join('data','type_keys.json'), 'r') as f:
    t_keys = json.load(f)


#creating/loading word2vec model
text8_corpus = api.load('text8')
try:
    w2v
except:
    w2v = Word2Vec(text8_corpus)

#creating NLP model for tokenizing, removing stopwords, extracting nouns etc.
nlp = English()
for k in t_keys:
    print('Starting {}'.format(k))
    if os.path.isfile(os.path.join('data','Short_T_w2v',k)):
        continue
    else:
        splits = split_doc(t_docs[k]['body'])
        tokens = set()
        wv = []
        for i in range(len(splits)-1):
            nlp_text = nlp(t_docs[k]['body'][splits[i]:splits[i+1]].lower())
            split_tokens=[token.text for token in nlp_text if not check_token(nlp,token.text)]
            tokens.update(split_tokens)
            wv.extend([w2v.wv[token] for token in split_tokens if token in w2v.wv.vocab])
        with open(os.path.join('data','Short_T_w2v',k),'wb') as f:
            pickle.dump(wv,f) 
