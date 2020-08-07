import numpy as np
import json
import mmdpdb, mongodb_database, redis_database
from mmdps.proc import loader, atlas
from mmdps.util import loadsave
import time,pymongo,pickle
import threading
import os
from mmdps import rootconfig
from mmdps.proc import atlas, loader
from mmdps.util import loadsave, clock

SCAN =['baihanxiang_20190307','caipinrong_20180412','baihanxiang_20190211','caochangsheng_20161027',
       'caochangsheng_20161114','chenguwen_20150711','chenhua_20150711','chenyifan_20150612',
       'chenyifan_20150629','cuichenhao_20180704','cuichenhao_20180820',
       'cuichenhao_20180913','cuiwei_20150825','daihuachen_20170323','daihuachen_20170426',
       'daihuachen_20170518','daishiqin_20180521','daishiqin_20180705',
       'daizhongxi_20181116','denghongbin_20181117','denghongbin_20181203','dingshuqin_20180802',
       'fengdaoliang_20160107','fengdaoliang_20160120','fuchenhao_20170602','fuchenhao_20170623']
DTNAMIC_SCAN=['CMSA_01','CMSA_02']
ATLAS=['aal','aicha','bnatlas','brodmann_lr','brodmann_lrce']
ATTR_FEATURE=['BOLD.BC', 'BOLD.CCFS', 'BOLD.LE','BOLD.net']
FEATURE=['bold_interBC','bold_interCCFS','bold_interLE','bold_interWD','bold_net']
DTNAMIC_FEATURE=['bold_net','bold_net_attr']
WINDOW_LENTH=[22,50,100]
STEP_SIZE=[1,3]


atlas_list = ['brodmann_lr', 'brodmann_lrce',
              'aal', 'aicha', 'bnatlas', 'aal2']

attr_list = ['BOLD.BC.inter', 'BOLD.CCFS.inter',
             'BOLD.LE.inter', 'BOLD.WD.inter', 'BOLD.net']

attr_name = ['bold_interBC.csv', 'bold_interCCFS.csv',
             'bold_interLE.csv', 'bold_interWD.csv', 'bold_net.csv']

dynamic_attr_list = ['inter-region_bc',
                     'inter-region_ccfs', 'inter-region_le', 'inter-region_wd']

attr_list_full = ['BOLD.BC.inter', 'BOLD.CCFS.inter', 'BOLD.LE.inter',
                  'BLD.WD.inter', 'BOLD.net.inter', 'DWI.FA', 'DWI.MD', 'DWI.net', 'DWI.MD', 'DWI.FA', 'DWI.net']

dynamic_conf_list = [[22, 1], [50, 1], [100, 1], [100, 3]]
def test_cache():
	db = mmdpdb.MMDPDatabase()
	# db.append_cache_list('test cache float', 1.123)
	# db.append_cache_list('test cache float', 2.234)
	lst = db.get_cache_list('test cache float')
	print(lst)

def test_get_dynamic_feature():
	a = mmdpdb.MMDPDatabase()
	a.get_dynamic_feature(DTNAMIC_SCAN,'brodmann_lrce','bold_net',22,1)

def insert_mongo(feat_name, dynamic_conf):
	atlasobj = atlas.get('brodmann_lrce')
	CMSA_dFeat = loader.load_dynamic_attr(loadsave.load_txt('/Users/andy/workshop/program_dynamic/CMSA_scan_list.txt'), atlasobj, 'inter-region_%s' % feat_name, dynamic_conf, rootFolder = '/Users/andy/workshop/MSADynamic/')
	NC_dFeat = loader.load_dynamic_attr(loadsave.load_txt('/Users/andy/workshop/program_dynamic/NC_scan_list.txt'), atlasobj, 'inter-region_%s' % feat_name, dynamic_conf, rootFolder = '/Users/andy/workshop/MSADynamic/')
	mdb = mongodb_database.MongoDBDatabase(data_source = 'MSA')
	for feat in CMSA_dFeat + NC_dFeat:
		mdb.save_dynamic_attr(feat)

def test_loader(feat_name, dynamic_conf):
	atlasobj = atlas.get('brodmann_lrce')
	CMSA_dFeat = loader.load_dynamic_attr(loadsave.load_txt('/Users/andy/workshop/program_dynamic/CMSA_scan_list.txt'), atlasobj, 'inter-region_%s' % feat_name, dynamic_conf, rootFolder = '/Users/andy/workshop/MSADynamic/')
	print(CMSA_dFeat[0].scan)
	print(CMSA_dFeat[0].data.shape)

def test_get_features():
	db = mmdpdb.MMDPDatabase(data_source = 'Changgung')
	feat = db.get_feature('chenyifan_20150629', 'brodmann_lrce', 'BOLD.BC')

	# mdb = mongodb_database.MongoDBDatabase(data_source = 'MSA')
	# feat = mdb.query_dynamic('MSA', 'CMSA_01', 'brodmann_lrce', 'BOLD.BC', 100, 3, slice_num = 1)

	print(feat.scan)
	print(feat.data.shape)

def compare_loader_database():
	db = mmdpdb.MMDPDatabase(data_source = 'Changgung')
	feat = db.get_feature('chenyifan_20150629', 'brodmann_lrce', 'BOLD.BC')
	feat_loader = loader.load_attrs(['chenyifan_20150629'], 'brodmann_lrce', 'BOLD.BC')[0]
	print(feat_loader.scan)
	print(feat_loader.data.shape)
	diff = np.abs(feat.data - feat_loader.data)
	print('maxdiff: ', np.max(diff))
'''
def mmdpdb_speed_test(data_source='Changgung'):
	"""
	    本程序请运行两遍，以测试mmdpdb的初始化和查询性能
	    redis目前只支持查询性能的测试，是否有必要测试存储性能
	"""
	mdb = mongodb_database.MongoDBDatabase(data_source)
	rdb = redis_database.RedisDatabase()
	mdpdb = mmdpdb.MMDPDatabase(data_source)
	loader_time = 0
	mongo_time = 0
	redis_time = 0
	mmdpdb_time = 0
	mriscans = os.listdir(rootconfig.path.feature_root)
	for mriscan in mriscans:
		atlas_path = os.path.join(rootconfig.path.feature_root, mriscan)
		for atlas_name in get_atlas_list(atlas_path, atlas_list):
			atlasobj = atlas.get(atlas_name)
			attr_path = os.path.join(atlas_path, atlas_name)
			for attr_name in get_attr_list(attr_path, attr_list):
				loader_start = time.time()
				attr = loader.load_attrs([mriscan], atlasobj, attr_name)
				loader_end = time.time()
				if mdb.exist_query('static', attr[0].scan, attr[0].atlasobj.name, attr[0].feature_name) != None:
					print(attr[0].scan, 'Please check again.')
				else:
					mongo_start = time.time()
					attrdata = pickle.dumps(attr[0].data)
					document = mdb.get_document(
						'static', attr[0].scan, attr[0].atlasobj.name, attr[0].feature_name, attrdata)
					mdb.db['features'].insert_one(document)
					mongo_end = time.time()
					loader_time += loader_end - loader_start
					mongo_time += mongo_end - mongo_start
					mmdpdb_start = time.time()
					mdpdb.get_feature(attr[0].scan, attr[0].atlasobj.name, attr[0].feature_name)
					mmdpdb_end = time.time()
					mmdpdb_time += mmdpdb_end - mmdpdb_start
					redis_start = time.time()
					rdb.get_static_value(data_source, attr[0].scan, attr[0].atlasobj.name, attr[0].feature_name)
					redis_end = time.time()
					redis_time += redis_end - redis_start
	print(loader_time)
	print(mongo_time)
	print(redis_time)
	print(mmdpdb_time)
	mdb.dbStats()
	mdb.colStats()
'''
def sqltest():
	with open('recordInformation.json', 'r', encoding='utf8') as fp:
		json_data = json.load(fp)
		a = mmdpdb.SQLiteDB('test.db')
		#a.init()
		#a.insert_eegrow(json_data)
		a.deleteGroupByName('nmsl')
if __name__ == '__main__':
	sqltest()
