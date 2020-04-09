import redis
import os
import numpy as np
import json
import pymongo
import pickle
import time
#from redis_database import RedisDatabase
from mongodb_database import MongoDBDatabase
from mmdps.proc import netattr , atlas, loader

SCAN =['baihanxiang_20190307','caipinrong_20180412','baihanxiang_20190211','caochangsheng_20161027',
       'caochangsheng_20161114','chenguwen_20150711','chenhua_20150711','chenyifan_20150612',
       'chenyifan_20150629','cuichenhao_20180704','cuichenhao_20180820',
       'cuichenhao_20180913','cuiwei_20150825','daihuachen_20170323','daihuachen_20170426',
       'daihuachen_20170518','daishiqin_20180521','daishiqin_20180705',
       'daizhongxi_20181116','denghongbin_20181117','denghongbin_20181203','dingshuqin_20180802',
       'fengdaoliang_20160107','fengdaoliang_20160120','fuchenhao_20170602','fuchenhao_20170623']
ATLAS=['aal','aicha','bnatlas','brodmann_lr','brodmann_lrce']
ATTR_FEATURE=['BOLD.BC', 'BOLD.CCFS', 'BOLD.LE','BOLD.net']
FEATURE=['bold_interBC','bold_interCCFS','bold_interLE','bold_interWD','bold_net']


def Inimongodb():#实验用mongodb数据库
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["TotalData"]
    mycol = mydb["features"]
    for i in SCAN:
        for j in ATLAS:
            for k in FEATURE:
                path = r'E:\Features' + '\\' + i + '\\' + j + '\\' + k + '.csv'  # 路径可以自己改
                if os.path.exists(path):
                    arr = np.loadtxt(path,delimiter=',')
                    mydict = {'scan': i, 'atlas': j, 'feature': k, 'dynamic': 'false', 'value': pickle.dumps(arr)}
                    x = mycol.insert_one(mydict)
def Inimongodb_dynamic():
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["TotalData"]
    mycol = mydb['dynamic_data']
    for i in SCAN:
        for j in ATLAS:
            for k in FEATURE:
                path = r'E:\Features' + '\\' + i + '\\' + j + '\\' + k + '.csv'  # 路径可以自己改
                if os.path.exists(path):
                    arr = np.loadtxt(path, delimiter=',')
                    mydict = {'scan': i, 'atlas': j, 'feature': k, 'dynamic': 'false', 'value': pickle.dumps(arr)}
                    x = mycol.insert_one(mydict)
def redis_speed_test():
    start=time.perf_counter()
    rdb=redis.Redis(host='127.0.0.1', port=6379, db=0)
    for i in SCAN:
        for j in ATLAS:
            for k in FEATURE:
                key=i+':'+j+':'+k+':0'
                value= rdb.get(key)
                rdb.expire(key, 1800)
    end = time.perf_counter()
    print('Redis running time: %s Seconds' % (end - start))
def monggo_speed_test():
    start = time.perf_counter()
    mdb = MongoDBDatabase()
    lst = []
    for i in SCAN:
        for j in ATLAS:
            for k in FEATURE:
                #if mdb.exists_static(i, j, k):
                    a = mdb.query_static(i, j, k)[0]
    end = time.perf_counter()
    print('MongoDB running time: %s Seconds' % (end - start))
def loader_speed_test():
    start = time.perf_counter()
    for i in SCAN:
        for j in ATLAS:
            atlasobj = atlas.get(j)
            net = loader.load_single_network(atlasobj, i)
    for j in ATLAS:
        atlasobj = atlas.get(j)
        for k in ATTR_FEATURE:
            attr = loader.load_attrs(SCAN, atlasobj, k)
    end = time.perf_counter()
    print('Loader running time: %s Seconds' % (end - start))
def size_test():
    rdb = redis.Redis(host='127.0.0.1', port=6379, db=15)
    N=1
    while (N<10000000):
        N=N*10
        rdb.delete('size')
        a=''
        for i in range(N):
            a=a+str(i)
        rdb.set('size',a)
        start = time.perf_counter()
        rdb.get('size')
        end = time.perf_counter()
        print(N ,'running time: %s Seconds' % (end - start))
def save_test():
    rdb=RedisDatabase()
    rdb.get_values(SCAN,ATLAS,FEATURE)
    start_idx = 0
    lst_1 = rdb.get_list_cache('test lst 1')
    if rdb.get_list_cache('test lst 1') is not None:
        print('rdb cached %d element in lst 1' % len(lst_1))
        start_idx = len(lst_1)
        print(lst_1)

    for idx in range(start_idx, 30):
        print('idx = %d' % idx)
        time.sleep(5)
        rdb.set_list_cache('test lst 1', idx)
        rdb.set_list_cache('test lst 2', idx)
    lst_1 = rdb.get_list_cache('test lst 1')
    lst_2 = rdb.get_list_cache('test lst 2')
    print(lst_1)
    print(lst_2)
    rdb.clear_cache('test lst 1')
    rdb.clear_cache('test lst 2')
def float_test():
    r = redis.StrictRedis(host='localhost', port=6379, db=15)
    r.set('name',4.3E-3)
    print(r.get('name'))
if __name__ == '__main__':
    #Inimongodb()
    #float_test()
    #a=RedisDatabase()
    #a.get_values(SCAN,ATLAS,FEATURE)
    #redis_speed_test()
    #monggo_speed_test()
    #loader_speed_test()
    #size_test()
    #save_test()
    for i in range(10):
        print(i)

