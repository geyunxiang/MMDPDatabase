import redis
import os
import numpy as np
import json
import pymongo
import pickle
import time
from redis_database import RedisDatabase
from mongodb_database import MongoDBDatabase
from mmdps.proc import netattr , atlas, loader
from mmdps import rootconfig

SCAN =['baihanxiang_20190307','caipinrong_20180412','baihanxiang_20190211','caochangsheng_20161027',
       'caochangsheng_20161114','chenguwen_20150711']
''','chenhua_20150711','chenyifan_20150612',
       'chenyifan_20150629','cuichenhao_20180704','cuichenhao_20180820',
       'cuichenhao_20180913','cuiwei_20150825','daihuachen_20170323','daihuachen_20170426',
       'daihuachen_20170518','daishiqin_20180521','daishiqin_20180705',
       'daizhongxi_20181116','denghongbin_20181117','denghongbin_20181203','dingshuqin_20180802',
       'fengdaoliang_20160107','fengdaoliang_20160120','fuchenhao_20170602','fuchenhao_20170623']'''
ATLAS=['aal','aicha','bnatlas','brodmann_lr','brodmann_lrce']
ATTR_FEATURE=['BOLD.BC', 'BOLD.CCFS', 'BOLD.LE','BOLD.net']
FEATURE=['bold_interBC','bold_interCCFS','bold_interLE','bold_interWD','bold_net']
feature_list = ['BOLD.BC', 'BOLD.CCFS', 'BOLD.LE', 'BOLD.WD']

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

def redis_speed_test():
    rdb = redis.Redis(host='127.0.0.1', port=6379, db=0)
    start = time.clock()
    for i in SCAN:
        for j in ATLAS:
            for k in FEATURE:
                key = i+':'+j+':'+k+':0'
                value = rdb.get(key)
                rdb.expire(key, 1800)
    end = time.clock()
    print('Redis running time: %s Seconds' % (end - start))

def redis_speed_test_network():
    rdb = RedisDatabase()
    mdb = MongoDBDatabase()
    # prepare redis first
    print('Preparing Redis...')
    for scan in os.listdir(rootconfig.path.feature_root):
        for atlas_name in ATLAS:
            query_result = mdb.query_static(scan, atlas_name, 'BOLD.net')
            if query_result is None:
                continue
            rdb.set_value(scan, atlas_name, 'BOLD.net', False, query_result['value'])
    # time redis performance
    print('Testing redis...')
    start = time.perf_counter()
    for scan in os.listdir(rootconfig.path.feature_root):
        for atlas_name in ATLAS:
            a = rdb.get_values(scan, atlas_name, 'BOLD.net')
    end = time.perf_counter()
    print('Redis running time: %s Seconds for network' % (end - start))
    # Redis running time: 2.277418057000002 Seconds for network (macOS, all scans, atlases)

def redis_speed_test_attr():
    rdb = RedisDatabase()
    mdb = MongoDBDatabase()
    # prepare redis first
    print('Preparing Redis...')
    for scan in os.listdir(rootconfig.path.feature_root):
        for atlas_name in ATLAS:
            for feature_name in feature_list:
                query_result = mdb.query_static(scan, atlas_name, feature_name)
                if query_result is None:
                    continue
                rdb.set_value(scan, atlas_name, feature_name, False, query_result['value'])
    # time redis performance
    print('Testing redis...')
    start = time.perf_counter()
    for scan in os.listdir(rootconfig.path.feature_root):
        for atlas_name in ATLAS:
            for feature_name in feature_list:
                a = rdb.get_values(scan, atlas_name, feature_name)
    end = time.perf_counter()
    print('Redis running time: %s Seconds for attr' % (end - start))
    # Redis running time: 5.145073779999997 Seconds for attr (macOS, all scans, atlases and features)

def mongo_speed_test_network():
    mdb = MongoDBDatabase()
    start = time.perf_counter()
    for scan in os.listdir(rootconfig.path.feature_root):
        for atlas_name in ATLAS:
            a = mdb.get_net(scan, atlas_name)
    end = time.perf_counter()
    print('MongoDB running time: %s Seconds for network' % (end - start))
    # MongoDB running time: 30.292381496999997 Seconds for network (macOS, all scans, atlases and features)

def mongo_speed_test_attr():
    mdb = MongoDBDatabase()
    start = time.perf_counter()
    for scan in os.listdir(rootconfig.path.feature_root):
        for atlas_name in ATLAS:
            for feature_name in feature_list:
                # print('scan: %s atlas_name: %s feature_name: %s' % (scan, atlas_name, feature_name))
                a = mdb.get_attr(scan, atlas_name, feature_name)
    end = time.perf_counter()
    print('MongoDB running time: %s Seconds for attr' % (end - start))
    # MongoDB running time: 71.608240657 Seconds for attr (macOS, all scans, atlases and features)

def loader_speed_test_network():
    start_net = time.perf_counter()
    for scan in os.listdir(rootconfig.path.feature_root):
        for atlas_name in ATLAS:
            atlasobj = atlas.get(atlas_name)
            try:
                net = loader.load_single_network(scan, atlasobj)
            except OSError as e:
                pass
    end_net = time.perf_counter()
    print('Loader running time: %s Seconds for network' % (end_net - start_net))
    # Loader running time: 72.7688368 Seconds for network (macOS, all scans, atlases)

def loader_speed_test_attr():
    start = time.perf_counter()
    scan_list = list(os.listdir(rootconfig.path.feature_root))
    for atlas_name in ATLAS:
        atlasobj = atlas.get(atlas_name)
        for feature_name in ATTR_FEATURE:
            try:
                attr = loader.load_attrs(scan_list, atlasobj, feature_name)
            except OSError as e:
                pass
    end = time.perf_counter()
    print('Loader running time: %s Seconds for attr' % (end - start))
    # Loader running time: 3.976306203 Seconds for attr (macOS, all scans, atlases and features)

def float_test():
    r = redis.StrictRedis(host='localhost', port=6379, db=15)
    r.set('name',4.3E-3)
    print(r.get('name'))

if __name__ == '__main__':
    #Inimongodb()
    #float_test()
    # a=RedisDatabase()
    # a.get_values(SCAN,ATLAS,FEATURE)
    redis_speed_test_network()
    # mongo_speed_test_network()
    # loader_speed_test_network()


