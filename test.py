import redis
import os
import numpy as np
import json
import pymongo
import pickle
SCAN =['baihanxiang_20190307','caipinrong_20180412','baihanxiang_20190211']
ATLAS=['aal','aal2','aicha','bnatlas','brodmann_lr','brodmann_lrce']
FEATURE=['bold_interBC','bold_interCCFS','bold_interLE','bold_interWD','bold_net','dwi_FA','dwi_MD','dwi_net','t1_GMD']


def Inimongodb():#实验用mongodb数据库
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["runoobdb"]
    mycol = mydb["static"]
    for i in SCAN:
        for j in ATLAS:
            for k in FEATURE:
                path = r'E:\Features' + '\\' + i + '\\' + j + '\\' + k + '.csv'  # 路径可以自己改
                if os.path.exists(path):
                    arr = np.loadtxt(path,delimiter=',')
                    mydict = {'scan': i, 'atlas': j, 'feature': k, 'dynamic': 0, 'content': pickle.dumps(arr)}
                    x = mycol.insert_one(mydict)

def GetDataFromMgdb(myquery={'scan':'caipinrong_20180412'}): #需要怎样的查询条件呢？
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["runoobdb"]
    mycol = mydb["static"]
    rdb = redis.Redis(host='127.0.0.1', port=6379, db=0)
    mydoc = mycol.find(myquery)
    for i in mydoc:
        key=i['scan']+':'+i['atlas']+':'+i['feature']+':0'
        rdb.set(key, (i['content'])[:-1])
    rdb.save()

def GetStaticData(myquery={'scan':'caipinrong_20180412','atlas':'aal','feature':'bold_interBC'}):
    rdb = redis.Redis(host='127.0.0.1', port=6379, db=0)
    key = myquery['scan']+':'+myquery['atlas']+':'+myquery['feature']+':0'
    csv = rdb.get(key)
    newcsv = Strtonp(csv.decode())
    return newcsv

def GetManyData(myquery={'scan':SCAN,'atlas':ATLAS,'feature':FEATURE}):
    rdb = redis.Redis(host='127.0.0.1', port=6379, db=0)
    dic={}
    for i in myquery['scan']:
        for j in myquery['atlas']:
            for k in myquery['feature']:
                key = 'Features:' + i + ':' + j + ':' + k+':0'
                csv=rdb.get(key)
                newcsv=Strtonp(csv.decode())
                dic[key]=newcsv
    return dic

def Strtonp(str):  #还是没有找到好的方法，暂时不改动（如果换一种存储方式可能会有其他的好的解码方法）
    list=str.split('\n')
    for i in range(len(list)):
        list[i]=list[i].split(',')
    nparray=np.array(list)
    if (nparray.shape[1]==1):
        nparray=nparray.T
    if (nparray.shape[0]==1):
        return nparray[0]
    else:
        return nparray
#Inimongodb()
'''
import time
rdb = redis.Redis(host='127.0.0.1', port=6379, db=1)
start =time.clock()
a=[]
for i in SCAN:
    for j in ATLAS:
        for k in FEATURE:
            key = i + ':' + j + ':' + k + ':0'
            csv = rdb.get(key)
            a.append(csv)
end = time.clock()
print('Running time: %s Seconds'%(end-start))
start =time.clock()
b=[]
for i in SCAN:
    for j in ATLAS:
        for k in FEATURE:
            key = i + ':' + j + ':' + k + ':0'
            b.append(key)
c=rdb.mget(b)
if a==c:
    print("yes")
else:
    print("no")
end = time.clock()
print('Running time: %s Seconds'%(end-start))
'''
a="Hello"
b=[]
b.append(a)
print(b)
c=[]
c.append('Hello')
print(c)

