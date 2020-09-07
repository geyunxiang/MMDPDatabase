import numpy as np
import mmdpdb, mongodb_database, redis_database
import time,pickle
import os, json
from mmdps import rootconfig
from mmdps.proc import atlas, loader

atlas_list = ['brodmann_lr', 'brodmann_lrce',
              'aal', 'aicha', 'bnatlas', 'aal2']

DynamicAtlas = ['brodmann_lrce']
#attr_list = ['bold_interBC','bold_interCCFS','bold_interLE','bold_interWD','bold_net']
attr_list = ['BOLD.BC.inter', 'BOLD.CCFS.inter',
             'BOLD.LE.inter', 'BOLD.WD.inter', 'BOLD.net']

dynamic_attr_list = ['inter-region_bc',
                     'inter-region_ccfs', 'inter-region_le', 'inter-region_wd']
attr_list_full = ['BOLD.BC.inter', 'BOLD.CCFS.inter', 'BOLD.LE.inter',
                  'BLD.WD.inter', 'BOLD.net.inter', 'DWI.FA', 'DWI.MD', 'DWI.net', 'DWI.MD', 'DWI.FA', 'DWI.net']

DynamiConf = [[22, 1], [50, 1], [100, 1], [100, 3]]



def LoadAttrNetTest_AttrNetTest(data_source='Changgung'):
	"""
	Test time usage of mongo and loader
	This function will check the folder's completeness
	"""
	MongoTime_attr = LoaderTime_attr = MongoTime_net = LoaderTime_net = 0
	mdb = mongodb_database.MongoDBDatabase(data_source)
	mriscans = os.listdir(rootconfig.path.feature_root)
	for mriscan in mriscans:
		for atlas_name in atlas_list:
			atlasobj = atlas.get(atlas_name)
			for attr_name in attr_list:
				try:
					if attr_name.find('net') != -1:
						loader_start = time.time()
						net = loader.load_single_network(mriscan, atlasobj)
						loader_end = time.time()
						LoaderTime_net += loader_end - loader_start
						if mdb.exist_query('static', net.scan, net.atlasobj.name, net.feature_name) != None:
							raise mongodb_database.MultipleRecordException(
								net.scan, 'Please check again.')
						doc = mdb.get_document(
							'static', net.scan, net.atlasobj.name, net.feature_name, pickle.dumps(net.data))
						mdb.db['features'].insert_one(doc)
						mongo_start = time.time()
						mdb.get_net(net.scan, net.atlasobj.name, net.feature_name)
						mongo_end = time.time()
						MongoTime_net += mongo_end - mongo_start
					else:
						loader_start = time.time()
						print([mriscan], atlasobj, attr_name)
						attr = loader.load_attrs([mriscan], atlasobj, attr_name)
						loader_end = time.time()
						LoaderTime_attr += loader_end - loader_start
						if mdb.exist_query('static', attr[0].scan, attr[0].atlasobj.name, attr[0].feature_name) != None:
							raise mongodb_database.MultipleRecordException(
								attr[0].scan, 'Please check again.')
						doc = mdb.get_document(
							'static', attr[0].scan, attr[0].atlasobj.name, attr[0].feature_name, pickle.dumps(attr[0].data))
						mdb.db['features'].insert_one(doc)
						mongo_start = time.time()
						mdb.get_attr(attr[0].scan, attr[0].atlasobj.name, attr[0].feature_name)
						mongo_end = time.time()
						MongoTime_attr += mongo_end - mongo_start
				except OSError:
					print('! not found! scan: %s, atlas: %s, attr: %s not found!' % (
						mriscan, atlas_name, attr_name))
				except mongodb_database.MultipleRecordException:
					print(
						'! Multiple record found ! scan: %s, atlas: %s, attr: %s' % (
							mriscan, atlas_name, attr_name))
	print('LoaderAttr: ',LoaderTime_attr)
	print('LoaderNet: ',LoaderTime_net)
	print('MongoAttr: ', MongoTime_attr)
	print('MongoNet: ', MongoTime_net)

def LoadDynamicAttrTest(rootfolder = rootconfig.path.dynamic_feature_root, data_source='Changgung'):
	loadertime = 0
	mriscans = os.listdir(rootfolder)
	for atlas_name in DynamicAtlas:
		atlasobj = atlas.get(atlas_name)
		for attrname in dynamic_attr_list:
			for dynamic_conf in DynamiConf:
				try:
					loadstart = time.time()
					loader.load_dynamic_attr(
						mriscans, atlasobj, attrname, dynamic_conf, rootfolder)
					loadend = time.time()
				except OSError:
					print('oserror')
				loadertime += loadend - loadstart
	print("LoaderDynamicAttr: ", loadertime)


def LoadDynamicNetTest(rootfolder = rootconfig.path.dynamic_feature_root, data_source='Changgung'):
	loadertime = 0
	mriscans = os.listdir(rootfolder)
	for mriscan in mriscans:
		for atlas_name in DynamicAtlas:
			atlasobj = atlas.get(atlas_name)
			for dynamic_conf in DynamiConf:
				try:
					loadstart = time.time()
					loader.load_single_dynamic_network(
						mriscan, atlasobj, dynamic_conf, rootfolder)
					loadend = time.time()
				except OSError:
					print('oserror')
				loadertime += loadend - loadstart
	print("LoaderDynamicNet: ", loadertime)


def DynamicAttrTest(rootfolder = rootconfig.path.dynamic_feature_root, data_source='Changgung'):
	""" Query and return dynamic attr object test"""
	mdb = mongodb_database.MongoDBDatabase(data_source)
	QueryTime = 0
	mriscans = os.listdir(rootfolder)
	for mriscan in mriscans:
		for atlas_name in DynamicAtlas:
			for dynamic_conf in DynamiConf:
				for attrname in dynamic_attr_list:
					try:
						query_start = time.time()
						mdb.get_dynamic_attr(
							mriscan, atlas_name, attrname, dynamic_conf[0], dynamic_conf[1])
						query_end = time.time()
						QueryTime += query_end - query_start
					except mongodb_database.NoRecordFoundException:
						print('! Not found! scan: %s, atlas: %s, attr: %s' %
							  (mriscan, atlas_name, attrname))
	print("MongoDynamicAttr: ", QueryTime)


def DynamicNetTest(rootfolder = rootconfig.path.dynamic_feature_root, data_source='Changgung'):
	""" Query and return dynamic net object test"""
	mdb = mongodb_database.MongoDBDatabase(data_source)
	QueryTime = 0
	mriscans = os.listdir(rootfolder)
	for mriscan in mriscans:
		for atlas_name in DynamicAtlas:
			for dynamic_conf in DynamiConf:
				try:
					query_start = time.time()
					mdb.get_dynamic_net(
						mriscan, atlas_name, dynamic_conf[0], dynamic_conf[1])
					query_end = time.time()
					QueryTime += query_end - query_start
				except mongodb_database.NoRecordFoundException:
					print('! Not found! scan: %s, atlas: %s' %
						  (mriscan, atlas_name))
	print("MongoDynamicNet: ", QueryTime)

def MMDPDBAttrTest_RedisAttrTest():
	rdb = redis_database.RedisDatabase()
	mdb = mmdpdb.MMDPDatabase()
	mriscans = os.listdir(rootconfig.path.feature_root)
	MMDPAttrTime = MMDPNetTime = RedisAttrTime = RedisNetTime = 0
	NetNum = AttrNum = 0
	for mriscan in mriscans:
		for atlas_name in atlas_list:
			for attr_name in attr_list:
				if attr_name.find('net') == -1:
					try:
						AttrNum += 1
						start = time.time()
						mdb.get_feature(mriscan, atlas_name, attr_name)
						MMDPAttrTime += time.time() - start
						start = time.time()
						rdb.get_static_value('Changgung',mriscan, atlas_name, attr_name)
						RedisAttrTime += time.time() - start
					except:
						AttrNum -= 1
				else:
					try:
						NetNum += 1
						start = time.time()
						mdb.get_feature(mriscan, atlas_name, attr_name)
						MMDPNetTime += time.time() - start
						start = time.time()
						rdb.get_static_value('Changgung', mriscan, atlas_name, attr_name)
						RedisNetTime += time.time() - start
					except:
						NetNum -= 1
	print('AttrDocumentNumber: ', AttrNum)
	print('NetDocumentNumber: ', NetNum)
	print("MMDPAttr: ", MMDPAttrTime)
	print("MMDPNet: ", MMDPNetTime)
	print("RedisAttr: ", RedisAttrTime)
	print("RedisNet: ", RedisNetTime)

def DynamicAttr():
	rdb = redis_database.RedisDatabase()
	mdb = mmdpdb.MMDPDatabase()
	MMDPTime = RedisTime = 0
	Num = 0
	mriscans = os.listdir(rootconfig.path.dynamic_feature_root)
	for mriscan in mriscans:
		for atlas_name in DynamicAtlas:
			for attrname in dynamic_attr_list:
				for dynamic_conf in DynamiConf:
					try:
						Num += 1
						start = time.time()
						mdb.get_dynamic_feature(mriscan, atlas_name, attrname, dynamic_conf[0], dynamic_conf[1])
						MMDPTime += time.time() - start
						start = time.time()
						rdb.get_dynamic_value('Changgung', mriscan, atlas_name, attrname, dynamic_conf[0],
											  dynamic_conf[1])
						RedisTime += time.time() - start
					except:
						Num -= 1
	print('DynamicAttrDocumentNumber: ', Num)
	print("MMDPDynamicAttr: ", MMDPTime)
	print("RedisDynamicAttr: ", RedisTime)

def DynamicNet():
	rdb = redis_database.RedisDatabase()
	mdb = mmdpdb.MMDPDatabase()
	MMDPTime = RedisTime = 0
	Num = 0
	mriscans = os.listdir(rootconfig.path.dynamic_feature_root)
	for mriscan in mriscans:
		for atlas_name in DynamicAtlas:
			for dynamic_conf in DynamiConf:
				try:
					Num += 1
					start = time.time()
					mdb.get_dynamic_feature(mriscan, atlas_name, 'BOLD.net', dynamic_conf[0], dynamic_conf[1])
					MMDPTime += time.time() - start
					start = time.time()
					rdb.get_dynamic_value('Changgung', mriscan, atlas_name, 'BOLD.net', dynamic_conf[0], dynamic_conf[1])
					RedisTime += time.time() - start
				except Exception as e:
					Num -= 1
	print('DynamicNetDocumentNumber: ', Num)
	print("MMDPDynamicNet: ", MMDPTime)
	print("RedisDynamicNet: ", RedisTime)

def sqltest():
	a = mmdpdb.SQLiteDB('mmdpdb.db')
	with open('recordInformation.json', encoding='utf8') as f:
		json_data = json.load(f)
		a.insert_eegrow(json_data)

if __name__ == '__main__':
	#LoadAttrNetTest_AttrNetTest()
	#LoadDynamicAttrTest()
	#LoadDynamicNetTest()
	#DynamicAttrTest()
	#DynamicNetTest()
	#MMDPDBAttrTest_RedisAttrTest()
	#DynamicAttr()
	#DynamicNet()
	sqltest()
