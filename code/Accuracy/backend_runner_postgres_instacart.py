import psycopg2
import argparse
import logging
import os
import pandas as pd
import time
import pickle
import sys
from psycopg2.extras import NamedTupleCursor
#os.chdir('../../')
#from setup import logger

import datetime
start_time = datetime.datetime.now()
print(start_time)
print("====================================")

# logger = logging.getLogger(__name__)
# parser = argparse.ArgumentParser()
# parser.add_argument("--verbose", dest='verbosity', help="increase output verbosity",
#                      action="store_true")
# parser.add_argument('-v',help='verbosity',dest='verbosity',action="store_true")
# # parser.add_argument('source')
# args = parser.parse_args()
# #
# if args.verbosity:
#     print("verbosity turned on")
#     handler = logging.StreamHandler(sys.stdout)
#     handler.setLevel(logging.DEBUG)
#     logger.addHandler(handler)
# #
# print(args.source)
if not os.path.exists('../../output/backend-postgres-actual/instacart-1000'):
        #logger.info('creating directory Accuracy')
        os.makedirs('../../output/backend-postgres-actual/instacart-1000')

if __name__=='__main__':
    print("main executing")
    with open('../../input/instacart_queries/queries-test-1000.pkl', 'rb') as f:
        queries = pickle.load(f)
    conn = psycopg2.connect(host='127.0.0.1',port=5433,dbname='instacart',user='postgres',password='tlssksek1!',cursor_factory=NamedTupleCursor)

    cur = conn.cursor()
    query_answers_dic = {}
    query_answers_dic['query_name'] = []
    query_answers_dic['time'] = []
    query_names = {}
    i = 0
    for qname,q in queries:
        print("Query {}".format(q))
        start = time.time()
        cur.execute(q)
        res = cur.fetchall()
        end = time.time()-start
        res_df = pd.DataFrame(res)
        res_df.to_pickle('../../output/backend-postgres-actual/instacart-1000/{}.pkl'.format(i))
        if qname not in query_names:
            query_names[qname] = [i]
        else:
            query_names[qname].append(i)
        query_answers_dic['time'].append(end)
        query_answers_dic['query_name'].append(qname)
        i+=1
    cur.close()
    conn.close()
    qa = pd.DataFrame(query_answers_dic)
    qa.to_csv('../../output/backend-postgres-actual/instacart-1000/query-response-time.csv')
    with open('../../output/backend-postgres-actual/instacart-1000/query-assoc-names.pkl', 'wb') as f:
        pickle.dump(query_names, f)


print("====================================")
end_time = datetime.datetime.now()
elapsed_time = end_time - start_time
sec_elapsed_time = elapsed_time.seconds
print("sec_elapsed_time : ", sec_elapsed_time, " sec")

ms_elapsed_time = elapsed_time.microseconds / 1000
print("ms_elapsed_time : ", ms_elapsed_time, " ms")