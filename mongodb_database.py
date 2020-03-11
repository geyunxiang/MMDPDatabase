"""
	MongoDB is a non-relational database used to store feature values.
	It stores data in JSON format, with a hierarchy of 
	db server -> database -> collection -> record. 

	The record looks like this:
	static
	{
	"scan": "baihanxiang_20190211",
	"atlas": "brodmann_lrce",
	"feature": "BOLD.inter.BC",
	"dynamic": false,
	"value": "...actual csv str...",
	"comment": "...descriptive str..."
	}

	dynamic
	{
	"scan": "CMSA_01",
	"atlas": "brodmann_lrce", 
	"feature": "BOLD.inter.BC",
	"dynamic": true, 
	"window length": 22,
	"step size": 1, 
	"value": [
		"1st str", 
		"2nd str",
		"...",
		"last str"
	],
	"comment": "...descriptive str..."
	}

    """

import pymongo
import numpy as np 
import csv
import os
import pickle
from mmdps.proc import loader,atlas
from mmdps.util import loadsave
from mmdps import rootconfig
#print("hello world")

root_folder=rootconfig.path.feature_root
#print(sorted(os.listdir(root_folder)))
#namelist=['baihanxiang','caipinrong']
#mriscans=loader.generate_mriscans(namelist)
atlas_list = ['brodmann_lr', 'brodmann_lrce', 'aal', 'aicha', 'bnatlas']
attr_list=['BOLD.BC','BOLD.CCFS','BOLD.LE','BLD.WD','BOLD.net','DWI.FA','DWI.MD','DWI.net']
#print(mriscans)
atlasobj = atlas.get('brodmann_lrce')
net = loader.load_single_network(atlasobj, 'baihanxiang_20190211')
#BOLD.net->bold_net.csv  
data_str = pickle.dumps(net.data)
attr = loader.load_attrs(['baihanxiang_20190211'], atlasobj, 'BOLD.BC')
#l=loader.AttrLoader(atlasobj,root_folder)
#attr1=l.loadSingle('baihanxiang_20190211','BOLD.BC')
#print(attr[0].data)
#print(pickle.dumps(attr[0].data))

class MongoDBDatabase:
	"""
	docstring for MongoDBDatabase
	"""
    
	def __init__(self,NameList):
		self.NameList=NameList
		self.client=pymongo.MongoClient('localhost',27017)
		self.db=self.client['TotalData-database']
		self.collection=self.db['csvdata-collection']
	    
	def generate_database(self): 
		"""
		Generate MongoDB from scratch. 
		Scan a directory and move the directory to MongoDB
		"""
		mriscans=loader.generate_mriscans(self.NameList)
		for mriscan in mriscans:
			for atlas_name in atlas_list:				
				atlasobj=atlas.get(atlas_name)
				for attr_name in attr_list:
					attr=loader.load_attrs(mriscan, atlasobj, attr_name)
					data_str=pickle.dumps(attr[0].data)	
					document={
						"scan":mriscan,
						"atlas":atlas_name,
						"feature":attr_name,
						"value":data_str,
						"dynamic":"false",
						"comment":""
						}
					self.collection.insert_one(document)

	if __name__ == '__main__':
		pass

	def generate_static_query(self,subject_name, scan_date , atlas_name , feature_name):
		m_query={}
		if subject_name != '' and scan_date != '':
			m_query["scan"] = subject_name + '_' + scan_date
		if atlas_name != '':
			m_query["atlas"] = atlas_name
		if feature_name != '':
			m_query["feature"] = feature_name
		m_query["dynamic"] = 0
		return m_query

	def query_static(self,subject_name, scan_date , atlas_name , feature_name):
		m_query = self.generate_static_query(subject_name, scan_date , atlas_name , feature_name)
		return self.collection.find(m_query)

	def exists_static(self,subject_name, scan_date, atlas_name , feature_name):
		return self.collection.count_documents(self.generate_static_query(subject_name, scan_date, atlas_name , feature_name))


	"""
	def old_loading_method():
	from mmdps.proc import loader, atlas
	import pickle
	atlasobj = atlas.get('brodmann_lrce')
	net = loader.load_single_network(atlasobj, 'baihanxiang_20190211')
	attr=loader.load_attrs(['baihanxiang_20190211'], atlasobj, 'BOLD.BC')
	data_str = pickle.dumps(net.data)
	"""
