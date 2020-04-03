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
import pickle
from mmdps.proc import loader, atlas, netattr

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

	def genarate_dynamic_query(self, subject_scan, atlas_name, feature_name,window_length,step_size):
		m_query = {}
		if subject_scan != '':
			m_query['scan'] = subject_scan
		if atlas_name != '':
			m_query['atlas'] = atlas_name
		if feature_name != '':
			m_query['feature'] = feature_name
		m_query['dynamic'] = 'true'
		if window_length !='':
			m_query['window length']=window_length
		if step_size !='':
			m_query['step size']=step_size
		return m_query

	def query_static(self, subject_scan, atlas_name, feature_name):
		self.col = self.db['features']
		m_query = self.generate_static_query(subject_scan, atlas_name, feature_name)
		return self.col.find_one(m_query)

	def query_dynamic(self, subject_scan, atlas_name, feature_name,window_length,step_size):
		self.col = self.db['dynamic_data']
		m_query = self.genarate_dynamic_query(subject_scan, atlas_name, feature_name,window_length,step_size)
		return self.col.find(m_query)

	def exists_static(self, subject_scan, atlas_name, feature_name):
		self.col = self.db['features']
		return self.col.count_documents(self.generate_static_query(subject_scan, atlas_name, feature_name))

	def exist_dynamic(self, subject_scan, atlas_name, feature_name,window_length,step_size):
		self.col = self.db['dynamic_data']
		return self.col.count_documents(self.genarate_dynamic_query(subject_scan, atlas_name, feature_name,window_length,step_size))

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

	def save_static_feature(self, feature):
		"""
		feature could be netattr.Net or netattr.Attr
		"""
		# check if feature already exist
		if self.exists_static(feature.scan, feature.atlasobj.name, feature.feature_name):
			raise MultipleRecordException()
		attrdata = pickle.dumps(feature.data)
		self.col = self.db['features']
		self.col.insert_one(self.generate_static_document(feature.scan, feature.atlasobj.name, feature.feature_name, attrdata))

	def remove_static_feature(self, scan, atlas_name, feature_name):
		self.col = self.db['features']
		self.col.find_one_and_delete(self.generate_static_query(scan, atlas_name, feature_name))

	def get_atlasobj(self,atlas_name):
		return atlas.get(atlas_name)

	def get_attr(self, subject_scan, atlas_name, feature_name):
		#directly return to an attrobj
		if self.exists_static(subject_scan,atlas_name,feature_name):
			binary_data = self.query_static(subject_scan, atlas_name, feature_name)['value']
			attrdata = pickle.loads(binary_data)
			atlasobj = atlas.get(atlas_name)
			attr = netattr.Attr(attrdata, atlasobj, subject_scan, feature_name)
			return attr
		else:
			print("can't find the document you look for. scan: %s, atlas_name: %s, feature_name: %s." % (subject_scan, atlas_name, feature_name))
			return None

	def get_net(self, subject_scan, atlas_name, featue_name = 'BOLD.net'):
		if self.exists_static(subject_scan, atlas_name, feature_name = 'BOLD.net'):
			binary_data = self.query_static(subject_scan, atlas_name, feature_name = 'BOLD.net')['value']
			netdata = pickle.loads(binary_data)
			atlasobj = atlas.get(atlas_name)
			net = netattr.Net(netdata, atlasobj, subject_scan, 'BOLD.net')
			return net
		else:
			print("can't find the document you look for. scan: %s, atlas_name: %s, feature_name: %s." % (subject_scan, atlas_name, feature_name))
			return None

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
