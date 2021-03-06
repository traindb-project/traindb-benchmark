import numpy as np
import lightgbm as lgb
import pandas as pd
import sys
import time
#os.chdir('../')
#sys.path.append('.')
from sql_parser.parser import QueryVectorizer

class MLAF:
    """
    Wrapper for ML models that act as estimator of aggregate functions
    """

    def predict_one(self, attr_dict):
        array = []
        for k in self.features:
            array.append(np.float(attr_dict.get(k, np.nan)))
   #     temp.append(m.predict_one(attr))
   #     query_vector = pd.Dataframe(array, columns=features)
        #print("****************")
        #print("array : ", array)
        #print("self.estimator : ", self.estimator)
        #print("len(self.features) : ", len(self.features))
        #print("self.estimator.num_feature() : ", self.estimator.num_feature())
        if len(self.features) > self.estimator.num_feature():
            array = np.delete(array, len(self.features)-1)
        #print("****************")

        return float(self.estimator.predict(np.array(array).reshape(1,-1)))

    def predict_many(self, attr_dict):
        start = time.time()
        qv = QueryVectorizer(self.features, SET_OWN=True)
        for a in attr_dict:
            qv.insert(a, attr_dict[a])

        d= qv._get_internal_representation()
        query_matrix =pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in d.items() ]))
        #print("****************")
        #print("self.estimator : ", self.estimator)
        #print("len(query_matrix.columns) : ", len(query_matrix.columns))
        #print("self.estimator.num_feature() : ", self.estimator.num_feature())
        if len(query_matrix.columns) > self.estimator.num_feature():
            query_matrix = query_matrix.drop(['bins'], axis=1)
        #print("****************")
#        if self.AF=='count':
#           query_matrix['product_name_lb'] = query_matrix['product_name_lb'].astype('int')
        print("Time for preprocessing {}".format(time.time()-start))
        return self.estimator.predict(query_matrix)

    def __init__(self, estimator, rel_error, feature_names, af):
        self.estimator = estimator
        self.AF = af
        self.rel_error = rel_error
        self.features = feature_names


if __name__=="__main__":
    est = MLAF(None,0.33,['f1_lb','f1_ub','f2_lb','f3_lb'],'count')
    print(est.features)
    est.predict_many({'f1_lb': 1, 'f2_lb':2, 'f3_lb':[4,45,6]})
