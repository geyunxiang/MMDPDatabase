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
       'caochangsheng_20161114','chenguwen_20150711','chenhua_20150711','chenyifan_20150612',
       'chenyifan_20150629','cuichenhao_20180704','cuichenhao_20180820',
       'cuichenhao_20180913','cuiwei_20150825','daihuachen_20170323','daihuachen_20170426',
       'daihuachen_20170518','daishiqin_20180521','daishiqin_20180705',
       'daizhongxi_20181116','denghongbin_20181117','denghongbin_20181203','dingshuqin_20180802',
       'fengdaoliang_20160107','fengdaoliang_20160120','fuchenhao_20170602','fuchenhao_20170623']
DTNAMIC_SCAN=['CMSA_01','CMSA_02','CMSA_03']
ATLAS=['aal','aicha','bnatlas','brodmann_lr','brodmann_lrce']
ATTR_FEATURE=['BOLD.BC', 'BOLD.CCFS', 'BOLD.LE','BOLD.net']
FEATURE=['bold_interBC','bold_interCCFS','bold_interLE','bold_interWD','bold_net']
feature_list = ['BOLD.BC', 'BOLD.CCFS', 'BOLD.LE', 'BOLD.WD']

DTNAMIC_FEATURE=['bold_net','bold_net_attr']
WINDOW_LENTH=[22,50,100]
STEP_SIZE=[1,3]

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
    for i in DTNAMIC_SCAN:
            for j in DTNAMIC_FEATURE:
                for k in WINDOW_LENTH:
                    for l in STEP_SIZE:
                        for n in range(200):
                            if j=='bold_net':
                                path = r'E:\Features'+'\\'+i+'\\'+'brodmann_lrce'+'\\'+j+'\\'+'dynamic '+str(l)+' '+str(k)+'\\'+'corrcoef-'+str(n*l)+'.'+str(k+n*l)+'.csv'
                            else:
                                path = r'E:\Features' + '\\' + i + '\\' + 'brodmann_lrce' + '\\' + j + '\\' + 'dynamic ' + str(l) + ' ' + str(k) + '\\' + 'inter-region_bc-' + str(n * l) + '.' + str(k + n * l) + '.csv'
                            if os.path.exists(path):
                                arr = np.loadtxt(path, delimiter=',')
                                mydict = {'scan':i,'atlas':'brodmann_lrce','feature':j,'dynamic':'true','window length':k,'step size':l,'no':n,'value':pickle.dumps(arr)}
                                x = mycol.insert_one(mydict)
                            else:
                                break
def redis_speed_test():
    start=time.perf_counter()
    rdb=redis.Redis(host='127.0.0.1', port=6379, db=0)
    for i in SCAN:
        for j in ATLAS:
            for k in FEATURE:
                key = i+':'+j+':'+k+':0'
                value = rdb.get(key)
                rdb.expire(key, 1800)
    end = time.perf_counter()
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
    for scan in os.listdir(rootconfig.path.feature_root):
        for atlas_name in ATLAS:
            atlasobj = atlas.get(atlas_name)
            for feature_name in ATTR_FEATURE:
                try:
                    attr = loader.load_attrs([scan], atlasobj, feature_name)
                except OSError as e:
                    pass
    end = time.perf_counter()
    print('Loader running time: %s Seconds for attr' % (end - start))
    # Loader running time: 77.37745339 Seconds for attr (macOS, all scans, atlases and features)

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

def nptest():
    a=np.array([1,0,0])
    b=np.array([0,1,0])
    d=np.array([0,0,9])
    c=[]
    c.append(a)
    c.append(b)
    c.append(d)
    print(np.linalg.det(np.array(c)))
def dynamic_test():
    start = time.perf_counter()
    a=RedisDatabase()
    for i in WINDOW_LENTH:
        for j in STEP_SIZE:
            b=a.get_values(DTNAMIC_SCAN,'brodmann_lrce',DTNAMIC_FEATURE,True,i,j)
    end = time.perf_counter()
    print('MongoDB running time: %s Seconds' % (end - start))
def pipe_test():
    r = redis.StrictRedis(host='localhost', port=6379, db=15)
    pipe = r.pipeline()
    lst=[]
    pipe.multi()
    for i in range(1,4,1):
        pipe.get(str(i))
    lst.append(pipe.execute())
    lst.append(pipe.execute())
    print(lst)

if __name__ == '__main__':
    #Inimongodb()
    #float_test()
    # a=RedisDatabase()
    # a.get_values(SCAN,ATLAS,FEATURE)
    # redis_speed_test_network()
    # mongo_speed_test_network()
    loader_speed_test_attr()
    #a=RedisDatabase()
    #a.get_values(SCAN,ATLAS,FEATURE)
    #redis_speed_test()
    #monggo_speed_test()
    #loader_speed_test()
    #size_test()
    #save_test()
    #nptest()
    #Inimongodb_dynamic()
    dynamic_test()
