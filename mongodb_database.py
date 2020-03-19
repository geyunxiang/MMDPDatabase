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


root_folder=rootconfig.path.feature_root
#mriscans=loader.generate_mriscans(namelist)
atlas_list = ['brodmann_lr', 'brodmann_lrce', 'aal', 'aicha', 'bnatlas']
attr_list=['BOLD.BC','BOLD.CCFS','BOLD.LE','BLD.WD','BOLD.net','DWI.FA','DWI.MD','DWI.net']
atlasobj = atlas.get('brodmann_lrce')
net = loader.load_single_network(atlasobj, 'baihanxiang_20190211')
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
    
	def __init__(self,host='localhost',port=27017,db="TotalData",col="features",password=''):
		self.client=pymongo.MongoClient(host,port)
		self.db=self.client[db]
		self.col=self.db[col]

	def generate_static_query(self,subject_scan,atlas_name,feature_name):
		m_query={}
		if subject_scan!='':
			m_query["scan"]=subject_scan
		if atlas_name!='':
			m_query["atlas"]=atlas_name
		if feature_name!='':
			m_query["feature"]=feature_name
		m_query["dynamic"]="false"
		return m_query
	
	def genarate_dynamic_query(self,subject_scan,atlas_name,feature_name):
		m_query={}
		if subject_scan!='':
			m_query["scan"]=subject_scan
		if atlas_name!='':
			m_query["atlas"]=atlas_name
		if feature_name!='':
			m_query["feature"]=feature_name
		m_query["dynamic"]="ture"
		return m_query


	
	def query_static(self,subject_scan,atlas_name,feature_name):
		self.col=self.db["features"]
		m_query=self.generate_static_query(subject_scan,atlas_name,feature_name)
		return self.col.find(m_query)

	def query_dynamic(self,subject_scan,atlas_name,feature_name):
		self.col=self.db["dynamic_data"]
		m_query=self.generate_dynamic_query(subject_scan,atlas_name,feature_name)
		return self.col.find(m_query)

	def exists_static(self,subject_scan, atlas_name , feature_name):
		self.col=self.db["features"]
		return self.col.count_documents(self.generate_static_query(subject_scan, atlas_name , feature_name))
	
	def exist_dynamic(self,subject_scan, atlas_name , feature_name):
		self.col=self.db["dynamic_data"]
		return self.col.count_documents(self.genarate_dynamic_query(subject_scan,atlas_name,feature_name))

	def generate_static_document(subject_scan,atlas_name,feature_name,value):
		static_documnet={
			"scan":subject_scan,
			"atlas":atlas_name,
			"feature":feature_name,
			"dynamic":"false",
			"value":value,
			"commment":''
		}
		return static_document

	def generate_dynamic_document(subject_scan,atlas_name,feature_name,value):
		dynamic_document={
			"scan":subject_scan,
			"atlas":atlas_name,
			"feature":feature_name,
			"dynamic":"ture",
			"window length": 22,
			"step size": 1, 
			"value":value,
			"commment":''
		}
		return dynamic_document
	
	def generate_dynamic_database(self,subject_scan,atlas_name,feature_name,value):
		#目前不知道动态数据的具体目录结构
		self.col=self.db["dynamic_data"]
		self.col.insert_one(self.generate_dynamic_document(subject_scan,atlas_name,feature_name,value))






def generate_static_database(): 
		"""
		Generate MongoDB from scratch. 
		Scan a directory and move the directory to MongoDB
		"""
	database=MongoDBDatabase()
	for mriscan in mriscans:
		for atlas_name in atlas_list:				
			atlasobj=atlas.get(atlas_name)
			for attr_name in attr_list:
				attr=loader.load_attrs(mriscan, atlasobj, attr_name)
				data_str=pickle.dumps(attr[0].data)
				database.col.insert_one(database.generate_static_documnet(meiscan,atlas_name,feature_name,data_str))	
	return database
	



if __name__ == '__main__':
	a=MongoDBDatabase()
	a.generate_database()



