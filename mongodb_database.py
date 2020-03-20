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
	"value": "...actual csv str...",
	"comment": "...descriptive str..."
}
"""

import pymongo
import numpy as np 
import csv
import os
import pickle
from mmdps.proc import loader, atlas

class MongoDBDatabase:
	"""
	docstring for MongoDBDatabase
	"""

	def __init__(self, host = 'localhost', port = 27017, db = 'TotalData', col = 'features', password = ''):
		self.client = pymongo.MongoClient(host, port)
		self.db = self.client[db]
		self.col = self.db[col]
		self.temp_db = self.client['Temp-database']
		self.temp_collection = self.temp_db['Temp-collection']

	def generate_static_query(self, subject_scan, atlas_name, feature_name):
		m_query = {}
		if subject_scan != '':
			m_query['scan'] = subject_scan
		if atlas_name != '':
			m_query['atlas'] = atlas_name
		if feature_name != '':
			m_query['feature'] = feature_name
		m_query['dynamic'] = 'false'
		return m_query

	def genarate_dynamic_query(self, subject_scan, atlas_name, feature_name):
		m_query = {}
		if subject_scan != '':
			m_query['scan'] = subject_scan
		if atlas_name != '':
			m_query['atlas'] = atlas_name
		if feature_name != '':
			m_query['feature'] = feature_name
		m_query['dynamic'] = 'true'
		return m_query

	def query_static(self, subject_scan, atlas_name, feature_name):
		self.col = self.db['features']
		m_query = self.generate_static_query(subject_scan, atlas_name, feature_name)
		return self.col.find(m_query)

	def query_dynamic(self, subject_scan, atlas_name, feature_name):
		self.col = self.db['dynamic_data']
		m_query = self.genarate_dynamic_query(subject_scan, atlas_name, feature_name)
		return self.col.find(m_query)

	def exists_static(self, subject_scan, atlas_name, feature_name):
		self.col = self.db['features']
		return self.col.count_documents(self.generate_static_query(subject_scan, atlas_name, feature_name))

	def exist_dynamic(self, subject_scan, atlas_name, feature_name):
		self.col = self.db['dynamic_data']
		return self.col.count_documents(self.genarate_dynamic_query(subject_scan, atlas_name, feature_name))

	def generate_static_document(self, subject_scan, atlas_name, feature_name, value):
		static_document = {
			'scan':subject_scan,
			'atlas':atlas_name,
			'feature':feature_name,
			'dynamic':'false',
			'value':value,
			'comment':''
		}
		return static_document

	def generate_dynamic_document(self, subject_scan, atlas_name, feature_name, value):
		dynamic_document = {
			'scan':subject_scan,
			'atlas':atlas_name,
			'feature':feature_name,
			'dynamic':'true',
			'window length': 22,
			'step size': 1, 
			'value':value,
			'commment':''
		}
		return dynamic_document

	def generate_dynamic_database(self, subject_scan, atlas_name, feature_name, value):
		#目前不知道动态数据的具体目录结构
		self.col = self.db['dynamic_data']
		self.col.insert_one(self.generate_dynamic_document(subject_scan, atlas_name, feature_name, value))
		#mongodb直接读取特征？

	def put_temp_data(self, temp_data, name, description = None):
		"""
		Insert temporary data into MongoDB. 
		Input temp_data as a serializable object (like np.array) and name as a string.
		The description argument is optional
		"""
		# check if name is already in temp database
		if self.temp_collection.count_documents(dict(name = name)) > 0:
			raise MultipleRecordException(name)
		document = dict(value = pickle.dumps(temp_data), name = name, description = description)
		self.temp_collection.insert_one(document)

	def get_temp_data(self, name):
		"""
		Get temporary data with name
		Return a dict with keys = value:np.array, name:str, description:str
		"""
		result = self.temp_collection.find_one(dict(name = name))
		result['value'] = pickle.loads(result['value'])
		return result

	def remove_temp_data(self, name):
		"""
		Delete all temp records with the input name
		If None is input, delete all temp data
		"""
		if name is None:
			self.temp_collection.delete_many({})
		else:
			self.temp_collection.delete_many(dict(name = name))

def generate_static_database(): 
	"""
	Generate MongoDB from scratch. 
	Scan a directory and move the directory to MongoDB
	"""
	database = MongoDBDatabase()
	for mriscan in mriscans:
		for atlas_name in atlas_list:
			atlasobj = atlas.get(atlas_name)
			for attr_name in attr_list:
				attr = loader.load_attrs(mriscan, atlasobj, attr_name)
				data_str = pickle.dumps(attr[0].data)
				database.col.insert_one(database.generate_static_document(mriscan, atlas_name, attr_name, data_str))
	return database

class MultipleRecordException(Exception):
	"""
	"""
	def __init__(self, name):
		self.name = name

	def __str__(self):
		return 'Multiple record found for name = ' + self.name + '. Please consider a new name.'

	def __repr__(self):
		return 'Multiple record found for name = ' + self.name + '. Please consider a new name.'

if __name__ == '__main__':
	pass
