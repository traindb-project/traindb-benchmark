import os
import sys
os.chdir('../../')
#print(os.listdir('.'))
sys.path.append('.')
import pickle
import psycopg2
import pandas as pd
from sql_parser.parser import Parser
import numpy as np
import time
import argparse

import datetime
start_time = datetime.datetime.now()
print(start_time)
print("====================================")

parser = argparse.ArgumentParser()
parser.add_argument("--custom", dest='custom', help="increase output verbosity",
                     action="store_true")
#parser.add_argument('--pass',dest='pass', help='pass connection password')
args = parser.parse_args()

if args.custom:
    if not os.path.exists('output/model-based-custom/instacart'):
            os.makedirs('output/model-based-custom/instacart')
else:
    if not os.path.exists('output/model-based/instacart-1000'):
            os.makedirs('output/model-based/instacart-1000')

with open('input/instacart_queries/queries-test-1000.pkl', 'rb') as f:
    queries = pickle.load(f)

with open('catalogues/distinct_attribute_catalogue.pkl', 'rb') as f:
    distinct_attr = pickle.load(f)

if args.custom:
    print("Using custom catalogue of models")
    with open('catalogues/model_catalogue_custom_objective.pkl', 'rb') as f:
        model_catalogue = pickle.load(f)
else:
    with open('catalogues/model_catalogue.pkl', 'rb') as f:
        model_catalogue = pickle.load(f)

with open('catalogues/labels_catalogue.pkl', 'rb') as f:
    labels_catalogue = pickle.load(f)

with open('catalogues/bin_catalogue.pkl', 'rb') as f:
    bin_catalogue = pickle.load(f)

print(model_catalogue)
query_answers_dic = {}
query_answers_dic['query_name'] = []
query_answers_dic['time'] = []
query_names = {}
i = 0
for qname,q in queries:
    print("Query : \n{}".format(q))
    #print(q)
    pr = Parser()
    pr.parse(q)
    dict_obj = pr.get_vector()
    proj_dict = pr.get_projections()
    print("proj_dict : ", proj_dict)
    gattr = pr.get_groupby_attrs()
    print("gattr : ", gattr)
    #print("dict_obj : ", dict_obj)
    #####
    # Estimation Phase
    res = {}
    start = time.time()
    for p in proj_dict:
        res[p] = []
        est = model_catalogue[p]
        #print("est : ", est)

        if len(gattr)>0:
            for g in gattr:
                #print("g : ", g)
                gvalues = distinct_attr[g]
                #print("gvalues : ", gvalues)
                #print("length of groupby values {}".format(len(gvalues)))
                res[g] = gvalues
                #print("res[g] : ", res[g])

                if g=='product_name':
                     dict_obj[g+'_lb'] = [labels_catalogue.get(gval,np.nan) for gval in gvalues]
                     dict_obj['bins'] = [bin_catalogue.get(labels_catalogue.get(gval),np.nan) for gval in gvalues]
                else:
                     dict_obj[g+'_lb'] = gvalues

                #print("dict_obj.columns : ", dict_obj)
                res[p]=est.predict_many(dict_obj)
                #print("res[p] : ", res[p])
        else:
            res[p].append(est.predict_one(dict_obj))
    end = time.time()-start

    #print("3333################")
    res_df = pd.DataFrame(res)
    print(res_df)
    print(res_df.describe())
    if args.custom:
        res_df.to_pickle('output/model-based-custom/instacart/{}.pkl'.format(i))
    else:
        res_df.to_pickle('output/model-based/instacart-1000/{}.pkl'.format(i))
    #####
    query_answers_dic['time'].append(end)
    query_answers_dic['query_name'].append(qname)
    if qname not in query_names:
        query_names[qname] = [i]
    else:
        query_names[qname].append(i)
    i+=1
    print("{}/{} Queries Processed ================".format(i,len(queries)))

qa = pd.DataFrame(query_answers_dic)
if args.custom:
    qa.to_csv('output/model-based-custom/instacart/query-response-time.csv')
    with open('output/model-based-custom/instacart/query-assoc-names.pkl', 'wb') as f:
        pickle.dump(query_names, f)
else:
    qa.to_csv('output/model-based/instacart-1000/query-response-time.csv')
    with open('output/model-based/instacart-1000/query-assoc-names.pkl', 'wb') as f:
        pickle.dump(query_names, f)



print("====================================")
end_time = datetime.datetime.now()
elapsed_time = end_time - start_time
sec_elapsed_time = elapsed_time.seconds
print("sec_elapsed_time : ", sec_elapsed_time, " sec")

ms_elapsed_time = elapsed_time.microseconds / 1000
print("ms_elapsed_time : ", ms_elapsed_time, " ms")