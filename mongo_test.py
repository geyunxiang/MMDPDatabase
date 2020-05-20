"""
MongoDB test script goes here.
"""
import os
import mongodb_database
import numpy as np
from mmdps import rootconfig
from mmdps.proc import atlas, loader

atlas_list = ['brodmann_lr', 'brodmann_lrce', 'aal', 'aicha', 'bnatlas']
attr_list_full = ['BOLD.BC', 'BOLD.CCFS', 'BOLD.LE', 'BLD.WD', 'BOLD.net', 'DWI.FA', 'DWI.MD', 'DWI.net']
attr_list = ['BOLD.BC', 'BOLD.CCFS', 'BOLD.LE', 'BOLD.WD', 'BOLD.net']
dynamic_attr_list=['inter-region_bc','inter-region_ccfs','inter-region_le','inter-region_wd']
dynamic_conf_list=[[22,1],[50,1],[100,1],[100,3]]

def generate_static_database_attrs(): 
	"""
	Generate MongoDB from scratch. 
	Scan a directory and move the directory to MongoDB
	"""
	database = mongodb_database.MongoDBDatabase('Changgung')
	mriscans = os.listdir(rootconfig.path.feature_root)
	for mriscan in mriscans:
		for atlas_name in atlas_list:
			atlasobj = atlas.get(atlas_name)
			for attr_name in attr_list:
				if attr_name.find('net') != -1:
					continue
				try:
					attr = loader.load_attrs([mriscan], atlasobj, attr_name)
					database.save_static_feature(attr[0])
					print('ok! scan: %s, atlas: %s, attr: %s ok!' % (mriscan, atlas_name, attr_name))
				except OSError as e:
					# print(e)
					print('! not found! scan: %s, atlas: %s, attr: %s not found!' % (mriscan, atlas_name, attr_name))

def generate_static_database_networks():
	database = mongodb_database.MongoDBDatabase('Changgung')
	mriscans = os.listdir(rootconfig.path.feature_root)
	for mriscan in mriscans:
		for atlas_name in atlas_list:
			atlasobj = atlas.get(atlas_name)
			try:
				net = loader.load_single_network(mriscan, atlasobj)
				# print(net.feature_name)
				database.save_static_feature(net)
				print('ok! scan: %s, atlas: %s, network ok!' % (mriscan, atlas_name))
			except OSError as e:
				# print(e)
				print('! not found! scan: %s, atlas: %s, network not found!' % (mriscan, atlas_name))

def generate_dynamic_database_attrs(dynamic_rootfolder):
	database=mongodb_database.MongoDBDatabase('Changgung')
	mriscans=os.listdir(dynamic_rootfolder)
	atlas_name = 'brodmann_lrce'
	atlasobj = atlas.get(atlas_name)
	for mriscan in mriscans:
		for dynamic_conf in dynamic_conf_list:
			for attrname in dynamic_attr_list:
				try:
					attr = loader.load_single_dynamic_attr(mriscan,atlasobj,attrname,dynamic_conf,dynamic_rootfolder)
					database.save_dynamic_attr(attr)
					print('ok! scans: %s, attr: %s ok!' % (mriscan, attrname))

				except OSError as e:
					# print(e)
					print('! not found! scans: %s, attr: %s not found!' % (mriscan, attrname))

def generate_dynamic_database_networks(dynamic_rootfolder):
	database=mongodb_database.MongoDBDatabase('Changgung')
	mriscans=os.listdir(dynamic_rootfolder)
	atlas_name = 'brodmann_lrce'
	atlasobj = atlas.get(atlas_name)
	for mriscan in mriscans:
		for dynamic_conf in dynamic_conf_list:
			try:
				net = loader.load_single_dynamic_network(mriscan,atlasobj,dynamic_conf,dynamic_rootfolder)
				database.save_dynamic_network(net)
				print('ok! scan: %s ok!' % (mriscan))
			except OSError as e:
				# print(e)
				print('! not found! scan: %s  not found!' % (mriscan))

def main():
	mdb = mongodb_database.MongoDBDatabase(None)
	mat = np.array([[1, 2, 3], [4, 5, 6]])
	# mdb.remove_temp_data('test')
	# mdb.put_temp_data(mat, 'test')

	res = mdb.get_temp_data('test')
	print(res)

def test_loading():
	root_folder = rootconfig.path.feature_root
	mriscans = ['baihanxiang_20190307', 'caipinrong_20180412', 'baihanxiang_20190211']
	"""
	atlasobj = atlas.get('brodmann_lrce')
	net = loader.load_single_network(atlasobj, 'baihanxiang_20190211')
	data_str = pickle.dumps(net.data)
	attr = loader.load_attrs(['baihanxiang_20190211'], atlasobj, 'BOLD.BC')
	#l=loader.AttrLoader(atlasobj,root_folder)
	#attr1=l.loadSingle('baihanxiang_20190211','BOLD.BC')
	#print(attr[0].data)
	#print(pickle.dumps(attr[0].data))
	"""

if __name__ == '__main__':
	generate_static_database_networks()
