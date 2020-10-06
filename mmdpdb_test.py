import numpy as np
import mmdpdb, mongodb_database, redis_database
import time,pickle
import os, json, csv
from mmdps import rootconfig
from mmdps.proc import atlas, loader
from mmdps.dms import tables
from sqlalchemy import create_engine, exists, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound


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
	Test time usage of mongo and loader when loading static attrs and networks
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
	"""
	Test time usage of loader when loading dynamic attrs
	"""
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
	"""
	Test time usage of loader when loading dynamic networks
	"""
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

def MongoDynamicAttrTest(rootfolder = rootconfig.path.dynamic_feature_root, data_source='Changgung'):
	"""
	Test time usage of MongoDB when loading dynamic attrs
	"""
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

def MongoDynamicNetTest(rootfolder = rootconfig.path.dynamic_feature_root, data_source='Changgung'):
	"""
	Test time usage of MongoDB when loading dynamic networks
	"""
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

def MMDPDBStaticAttr(feature_root = rootconfig.path.feature_root):
	"""
	Test time usage of MMDPDatabase and Redis when loading static attrs
	"""
	rdb = redis_database.RedisDatabase()
	db = mmdpdb.MMDPDatabase()
	mriscans = os.listdir(feature_root)

	load_counter = 0
	query_start = time.time()
	for mriscan in mriscans:
		for atlas_name in atlas_list:
			for attr_name in attr_list:
				if attr_name.find('net') != -1:
					continue
				try:
					attr = db.get_feature(mriscan, atlas_name, attr_name)
					load_counter += 1
				except mongodb_database.NoRecordFoundException:
					pass
					# print('! not found! scan: %s, atlas: %s, attr: %s not found!' % (mriscan, atlas_name, attr_name))
	query_end = time.time()
	query_time = query_end - query_start
	print('Query %d static attrs (netattr.Attr) using MMDPDatabase time cost: %1.2fs' % (load_counter, query_time))
	print('Redis get cost: %.2f' % mmdpdb.static_rdb_get)
	print('Redis set cost: %.2f' % mmdpdb.static_rdb_set)
	print('Mongo get cost: %.2f' % mmdpdb.static_mdb_get)

	load_counter = 0
	query_start = time.time()
	for mriscan in mriscans:
		for atlas_name in atlas_list:
			for attr_name in attr_list:
				if attr_name.find('net') != -1:
					continue
				attr = rdb.get_static_value('Changgung', mriscan, atlas_name, attr_name)
				if attr is None:
					pass
					# print('! not found! scan: %s, atlas: %s, attr: %s not found!' % (mriscan, atlas_name, attr_name))
				else:
					load_counter += 1
	query_end = time.time()
	query_time = query_end - query_start
	print('Query %d static attrs (netattr.Attr) using RedisDatabase time cost: %1.2fs' % (load_counter, query_time))

def MMDPDBStaticNet(feature_root = rootconfig.path.feature_root):
	"""
	Test time usage of MMDPDatabase and Redis when loading static networks
	"""
	rdb = redis_database.RedisDatabase()
	db = mmdpdb.MMDPDatabase()
	mriscans = os.listdir(feature_root)

	load_counter = 0
	query_start = time.time()
	for mriscan in mriscans:
		for atlas_name in atlas_list:
			try:
				attr = db.get_feature(mriscan, atlas_name, 'BOLD.net')
				load_counter += 1
			except mongodb_database.NoRecordFoundException:
				pass
				# print('! not found! scan: %s, atlas: %s, networks not found!' % (mriscan, atlas_name))
	query_end = time.time()
	query_time = query_end - query_start
	print('Query %d static networks (netattr.Net) using MMDPDatabase time cost: %1.2fs' % (load_counter, query_time))
	print('Redis get cost: %.2f' % mmdpdb.static_rdb_get)
	print('Redis set cost: %.2f' % mmdpdb.static_rdb_set)
	print('Mongo get cost: %.2f' % mmdpdb.static_mdb_get)

	load_counter = 0
	query_start = time.time()
	for mriscan in mriscans:
		for atlas_name in atlas_list:
			attr = rdb.get_static_value('Changgung', mriscan, atlas_name, 'BOLD.net')
			if attr is None:
				pass
				# print('! not found! scan: %s, atlas: %s, networks not found!' % (mriscan, atlas_name))
			else:
				load_counter += 1
	query_end = time.time()
	query_time = query_end - query_start
	print('Query %d static attrs (netattr.Attr) using RedisDatabase time cost: %1.2fs' % (load_counter, query_time))

def MMDPDBDynamicAttr(feature_root = rootconfig.path.dynamic_feature_root):
	"""
	Test time usage of MMDPDatabase and Redis when loading dynamic attrs
	"""
	rdb = redis_database.RedisDatabase()
	db = mmdpdb.MMDPDatabase('MSA')
	mriscans = os.listdir(feature_root)
	atlas_name = 'brodmann_lrce'
	attr_name = 'BOLD.BC.inter'
	dynamic_conf = (22, 1)

	load_counter = 0
	query_start = time.time()
	for mriscan in mriscans:
		try:
			attr = db.get_dynamic_feature(mriscan, atlas_name, attr_name, dynamic_conf[0], dynamic_conf[1])
			load_counter += 1
		except mongodb_database.NoRecordFoundException:
			pass
			# print('! not found! scan: %s, atlas: %s, attr: %s not found!' % (mriscan, atlas_name, attr_name))
	query_end = time.time()
	query_time = query_end - query_start
	print('Query %d dynamic attrs (netattr.DynamicAttr) using MMDPDatabase time cost: %1.2fs' % (load_counter, query_time))
	print('Redis get cost: %.2f' % mmdpdb.dynamic_rdb_get)
	print('Redis set cost: %.2f' % mmdpdb.dynamic_rdb_set)
	print('Mongo get cost: %.2f' % mmdpdb.dynamic_mdb_get)

	load_counter = 0
	query_start = time.time()
	for mriscan in mriscans:
		attr = rdb.get_dynamic_value('MSA', mriscan, atlas_name, attr_name, dynamic_conf[0], dynamic_conf[1])
		if attr is None:
			pass
			# print('! not found! scan: %s, atlas: %s, attr: %s not found!' % (mriscan, atlas_name, attr_name))
		else:
			load_counter += 1
	query_end = time.time()
	query_time = query_end - query_start
	print('Query %d dynamic attrs (netattr.DynamicAttr) using RedisDatabase time cost: %1.2fs' % (load_counter, query_time))

def MMDPDBDynamicNet(feature_root = rootconfig.path.dynamic_feature_root):
	"""
	Test time usage of MMDPDatabase and Redis when loading dynamic networks
	"""
	rdb = redis_database.RedisDatabase()
	db = mmdpdb.MMDPDatabase('MSA')
	mriscans = os.listdir(feature_root)
	atlas_name = 'brodmann_lrce'
	dynamic_conf = (22, 1)
	load_counter = 0
	query_start = time.time()
	for mriscan in mriscans:
		try:
			attr = db.get_dynamic_feature(mriscan, atlas_name, 'BOLD.net', dynamic_conf[0], dynamic_conf[1])
			load_counter += 1
		except mongodb_database.NoRecordFoundException:
			pass
			# print('! not found! scan: %s, atlas: %s, networks not found!' % (mriscan, atlas_name))
	query_end = time.time()
	query_time = query_end - query_start
	print('Query %d dynamic networks (netattr.DynamicNet) using MMDPDatabase time cost: %1.2fs' % (load_counter, query_time))
	print('Redis get cost: %.2f' % mmdpdb.dynamic_rdb_get)
	print('Redis set cost: %.2f' % mmdpdb.dynamic_rdb_set)
	print('Mongo get cost: %.2f' % mmdpdb.dynamic_mdb_get)

	load_counter = 0
	query_start = time.time()
	for mriscan in mriscans:
		attr = rdb.get_dynamic_value('MSA', mriscan, atlas_name, 'BOLD.net', dynamic_conf[0], dynamic_conf[1])
		if attr is None:
			pass
			# print('! not found! scan: %s, atlas: %s, networks not found!' % (mriscan, atlas_name))
		else:
			load_counter += 1
	query_end = time.time()
	query_time = query_end - query_start
	print('Query %d dynamic networks (netattr.DynamicNet) using RedisDatabase time cost: %1.2fs' % (load_counter, query_time))
if __name__ == '__main__':
	# LoadAttrNetTest_AttrNetTest()
	# LoadDynamicAttrTest()
	# LoadDynamicNetTest()
	# MongoDynamicAttrTest()
	# MongoDynamicNetTest()
	#for num in range(2):
	#	print('Round %d' %(num + 1))
		# MMDPDBStaticAttr()
		# MMDPDBStaticNet()
		#MMDPDBDynamicAttr()
	#	MMDPDBDynamicNet()
	pass