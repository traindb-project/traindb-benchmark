import os
import sys
os.chdir('../../')
#print(os.listdir('.'))
sys.path.append('.')
import pickle
import pandas as pd
import numpy as np
from ml.model import MLAF
import xgboost as xgb
import time
from sklearn.model_selection import train_test_split
import lightgbm as lgb




if not os.path.exists('output/performance'):
        print('creating ', 'performance')
        os.makedirs('output/performance')
if not os.path.exists('output/performance/csvs'):
        print('creating ', 'performance csvs')
        os.makedirs('output/performance/csvs')
if not os.path.exists('output/performance/complexity_store'):
        print('creating ', 'performance complexity_store')
        os.makedirs('output/performance/complexity_store')

import datetime
start_time = datetime.datetime.now()
print(start_time)
print("====================================")


#qdf = pd.read_pickle('input/instacart_queries/qdf.pkl')
qdf = pd.read_pickle('input/instacart_queries/qdf-1000.pkl')
targets = [name  for name in qdf.columns if 'lb' not in name and 'ub' not in name]
#Filtering out the product of joins in the aggregate functions
count_columns = [name for name in qdf[targets].columns if 'count' in name]
#Generate Dataframes per aggregate function
count_df = qdf.iloc[qdf['count'].dropna(axis=0).index]
count_df = count_df.iloc[:10000]
features = [name for name in count_df.columns if name not in ['sum_add_to_cart_order','avg_add_to_cart_order','count']]


target_count = 'count'
count_df = count_df[(count_df[target_count]!=0)] # Remove 0 because it produces an error on relative error
count_df['product_name_lb'] = count_df['product_name_lb'].replace(np.nan, 'isnone')
count_df['product_name_lb'] = count_df['product_name_lb'].astype('category')
labels =  count_df['product_name_lb'].cat.codes
categorical_attribute_catalogue = {key : value for key,value in zip(count_df['product_name_lb'].values, labels)}
count_df['product_name_lb'] = labels

del qdf

X = count_df[features].values[:10000]
y = count_df['count'].values

#model = lgb.LGBMRegressor()
lgb_model = lgb.LGBMRegressor(n_estimators=5000)

print("Number of total rows : {}".format(count_df.shape[0]))
query_results = {}
query_results['boosting'] = []
query_results['time'] = []
query_results['size'] = []
boosting = np.linspace(500, 20000, 20)
# # read in data
for b in boosting:
    for i in range(10):
        start = time.time()

        #lgb_model = lgb.fit(n_estimators=b)
        lgb_model.fit(X,y)

        end = time.time()-start
        query_results['boosting'].append(b)
        query_results['time'].append(end)
        pkl_filename = "pickle_model.pkl"
        with open(pkl_filename, 'wb') as file:
            pickle.dump(lgb_model, file)
        statinfo = os.stat('pickle_model.pkl')
        query_results['size'].append(statinfo.st_size)
    print("Time to train for {} \t took : {}+-".format((b,end), np.mean(query_results['time']), np.std(query_results['time'])))

    # xgb_model.save_model('/home/fotis/dev_projects/model-based-aqp/catalogues/{}.dict_model'.format(label))
df = pd.DataFrame(query_results)
df.to_csv('output/performance/csvs/complexity_overhead.csv')


print("====================================")
end_time = datetime.datetime.now()
elapsed_time = end_time - start_time
sec_elapsed_time = elapsed_time.seconds
print("sec_elapsed_time : ", sec_elapsed_time, " sec")

ms_elapsed_time = elapsed_time.microseconds / 1000
print("ms_elapsed_time : ", ms_elapsed_time, " ms")