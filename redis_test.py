import redis
import os
import numpy as np
import json
import pymongo
import pickle
from mmdps import rootconfig


SCAN =['baihanxiang_20190307','caipinrong_20180412','baihanxiang_20190211','caochangsheng_20161027',
       'caochangsheng_20161114','chenguwen_20150711','chenhua_20150711','chenyifan_20150612',
       'chenyifan_20150629','cuichenhao_20180704','cuichenhao_20180820',
       'cuichenhao_20180913','cuiwei_20150825','daihuachen_20170323','daihuachen_20170426',
       'daihuachen_20170518','daishiqin_20180521','daishiqin_20180705',
       'daizhongxi_20181116','denghongbin_20181117','denghongbin_20181203','dingshuqin_20180802',
       'fengdaoliang_20160107','fengdaoliang_20160120','fuchenhao_20170602','fuchenhao_20170623']
ATLAS=['aal','aicha','bnatlas','brodmann_lr','brodmann_lrce']
FEATURE=['bold_interBC','bold_interCCFS','bold_interLE','bold_interWD','bold_net']
DTNAMIC_FEATURE=['inter-region_bc','inter-region_ccfs', 'inter-region_le', 'inter-region_wd']
WINDOW_LENTH=[22,50,100]
STEP_SIZE=[1,3]

def Inimongodb():#实验用mongodb数据库
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient['Changgung']
    mycol = mydb["features"]
    for i in SCAN:
        for j in ATLAS:
            for k in FEATURE:
                path = r'E:\MRIscan\Features' + '\\' + i + '\\' + j + '\\' + k + '.csv'  # 路径可以自己改
                if os.path.exists(path):
                    arr = np.loadtxt(path,delimiter=',')
                    mydict = {'data_source': 'Changgung', 'scan': i, 'atlas': j, 'feature': k, 'dynamic': 0, 'value': pickle.dumps(arr), 'comment': {}}
                    x = mycol.insert_one(mydict)
def Inimongodb_dynamic():
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient['Changgung']
    mycol_attr = mydb['dynamic_attr']
    mycol_net = mydb['dynamic_net']
    DTNAMIC_SCAN = list(os.listdir(rootconfig.path.dynamic_feature_root))
    for i in DTNAMIC_SCAN:
        for k in WINDOW_LENTH:
            for l in STEP_SIZE:
                for n in range(200):
                    path = r'E:\MRIscan\Dynamic_features' + '\\' + i + '\\' + 'brodmann_lrce\\bold_net\\dynamic ' + str(l) + ' ' + str(k) + '\\' + 'corrcoef-' + str(n * l) + '.' + str(k + n * l) + '.csv'
                    if os.path.exists(path):
                        arr = np.loadtxt(path, delimiter=',')
                        mydict = {'data_source': 'Changgung', 'scan': i, 'atlas': 'brodmann_lrce', 'feature': 'BOLD.net',
                                  'dynamic': 1, 'window_length': k, 'step_size': l, 'slice_num': n,
                                  'value': pickle.dumps(arr), 'comment': {}}
                        x = mycol_net.insert_one(mydict)
                    else:
                        break
                    for j in DTNAMIC_FEATURE:
                        path = r'E:\MRIscan\Dynamic_features' + '\\' + i + '\\' + 'brodmann_lrce\\bold_net_attr\\dynamic ' + str(l) + ' ' + str(k) + '\\' + j + '-' + str(n * l) + '.' + str(k + n * l) + '.csv'
                        if os.path.exists(path):
                            arr = np.loadtxt(path, delimiter=',')
                            mydict = {'data_source': 'Changgung','scan':i,'atlas':'brodmann_lrce','feature':j,'dynamic':1,'window_length':k,'step_size':l,'slice_num':n,'value':pickle.dumps(arr), 'comment': {}}
                            x = mycol_attr.insert_one(mydict)
                        else:
                            break


if __name__ == '__main__':
    #Inimongodb()
    #Inimongodb_dynamic()
    rdb = redis.StrictRedis(host='localhost', port=6379, db=4)
    with open('The.Hot.Zone.S01E06.Hidden.720p.AMZN.WEB-DL.DDP5.1.H.264-NTG.mkv', 'rb') as f:
        rdb.set('movie', f.read())

