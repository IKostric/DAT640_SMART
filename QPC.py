import numpy as np
import os
import json

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.neural_network import MLPClassifier
import pickle

class QPC_model:
    def __init__(self):
        '''
        Arguments
            conf:         Configuration ID for saving or loading model
            ngram_range:  Ngrams to use when tokenizing
            min_df:       Minimum document frequency
        '''
        self.ngram_range = (1,2)
        self.min_df = 1
        self.classes = np.array(['resource', 'date', 'number', 'string', 'boolean'])
        self.mlpc = None
        self.conf = 'tc1'
        
    def model(self):
        '''
        Method for loading word vectorizer and neural network.
        '''
        self.cv, self.mlpc = self._load_model(self.conf)


    def predict(self, query_list):
        '''
        Method for predicting category labels.
        Pass query_list as list of dictionaries with queries.
        '''
        self.model()
        if not self.mlpc:
            print('Model not loaded')
            return None
        queries = [q['question'] for q in query_list if q['question'] is not None]
        q_IDs = [q['id'] for q in query_list if q['id'] is not None]
        vec = self.cv.transform(queries)
        pred = self.mlpc.predict(vec)
        pred_dict = {}
        for i, q_ID in enumerate(q_IDs):
            pred_dict[q_ID] = pred[i]
        return pred_dict
        
    def _load_model(self, conf):
        with open('qpccv-'+ conf + '.sav','rb') as f:
            cv = pickle.load(f)
        with open('qpcmlpc-' + conf + '.sav','rb') as f:
            mlpc = pickle.load(f)
        return (cv, mlpc)