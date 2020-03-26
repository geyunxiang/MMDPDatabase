import redis
import os
import numpy as np
import json
import pymongo
import pickle
import time
from redis_database import RedisDatabase
SCAN =['baihanxiang_20190307','caipinrong_20180412','baihanxiang_20190211','caochangsheng_20161027',
       'caochangsheng_20161114','chenguwen_20150711','chenhua_20150711','chenyifan_20150612',
       'chenyifan_20150629','chenyifan_20150923','cuichenhao_20180704','cuichenhao_20180820',
       'cuichenhao_20180913','cuiwei_20150825','daihuachen_20170323','daihuachen_20170426',
       'daihuachen_20170518','daishiqin_20180302','daishiqin_20180302','daishiqin_20180705',
       'daizhongxi_20181116','denghongbin_20181117','denghongbin_20181203','denghongbin_20181203',
       'denghongbin_20181203','fengdaoliang_20160120','fuchenhao_20170602','fuchenhao_20170623']
ATLAS=['aal','aal2','aicha','bnatlas','brodmann_lr','brodmann_lrce']
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


def speed_test():
    rdb = redis.Redis(host='127.0.0.1', port=6379, db=0)
    start =time.clock()
    a=[]
    for i in SCAN:
        for j in ATLAS:
            for k in FEATURE:
                key = i + ':' + j + ':' + k + ':0'
                csv = rdb.get(key)
                #rdb.expire(key,1800)
                a.append(csv)
    end = time.clock()
    print('Running time: %s Seconds'%(end-start))
    b=[]
    for i in SCAN:
        for j in ATLAS:
            for k in FEATURE:
                key = i + ':' + j + ':' + k + ':0'
                b.append(key)
    start = time.clock()
    c=rdb.mget(b)
    #for i in c:
     #   rdb.expire(i,1800)
    end = time.clock()
    print('Running time: %s Seconds'%(end-start))
if __name__ == '__main__':
    #Inimongodb()
    a=RedisDatabase()
    #start=time.clock()
    #print(a.get_values(SCAN,ATLAS,FEATURE))
    #end = time.clock()
    #print('Running time: %s Seconds' % (end - start))
    #speed_test()
    a.set_list_cash_all('hahaha',['1','2'])
    print(a.get_list_cash('hahaha'))


