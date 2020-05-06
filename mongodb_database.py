"""
MongoDB is a non-relational database used to store feature values.
It stores data in JSON format, with a hierarchy of 
db server -> database -> collection -> record. 

The record looks like this:
static
{
	"data_source: Changgung",
	"scan": "baihanxiang_20190211",
	"atlas": "brodmann_lrce",
	"feature": "BOLD.inter.BC",
	"dynamic": 0,
	"value": "...actual csv str...",
	"comment": "...descriptive str..."
}

dynamic
{
	"data_source:Changgung",
	"scan": "CMSA_01",
	"atlas": "brodmann_lrce", 
	"feature": "BOLD.inter.BC",
	"value": "...actual csv str...",
	"dynamic": 1, 
	"window_length": 22,
	"step_size": 1, 
	"slice_num": the num of the slice 0,1,2,3…
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

	def __init__(self, host = 'localhost', port = 27017, db = 'ChangGeng', col = 'features', password = ''):
		self.client = pymongo.MongoClient(host, port)
		self.db = self.client[db]
		self.col = self.db[col]
		self.temp_db = self.client['Temp-database']
		self.temp_collection = self.temp_db['Temp-collection']

	"""
	default data_source/database : ChangGeng;
	default collection : features;
	"""

	def generate_static_query(self,data_source ='Changgung',subject_scan, atlas_name, feature_name):
		static_query=dict(data_source=data_source,scan=subject_scan,atlas=atlas_name,feature=feature_name,dynamic=0)
		return static_query

	def genarate_dynamic_query(self,data_source ='Changgung', subject_scan, atlas_name, feature_name,window_length,step_size):
		dynamic_query=dict(data_source=data_source,scan=subject_scan,atlas=atlas_name,feature=feature_name,dynamic=1,window_length=window_length,step_size=step_size)
		return dynamic_query

	def query_static(self,data_source ='Changgung',subject_scan, atlas_name, feature_name):
		static_query= self.generate_static_query(data_source,subject_scan, atlas_name, feature_name)
		self.col=self.db['features']
		return self.col.find(static_query)

	def query_dynamic(self,data_source ='Changgung',subject_scan, atlas_name, feature_name,window_length,step_size,slice_num):
		dynamic_query = self.genarate_dynamic_query(data_source,subject_scan, atlas_name, feature_name,window_length,step_size,slice_num)
		self.col=self.db['dynamic_data']
		return self.col.find(dynamic_query).sort("no",1)

	def exist_static(self,data_source='Changgung', subject_scan, atlas_name, feature_name):
		self.col=self.db['features']
		return self.col.count_documents(self.generate_static_query(data_source,subject_scan, atlas_name, feature_name))

	def exist_dynamic(self,data_source='Changgung',subject_scan, atlas_name, feature_name,window_length,step_size,slice_num):
		self.col=self.db['dynamic_data']
		return self.col.count_documents(self.genarate_dynamic_query(data_source,subject_scan, atlas_name, feature_name,window_length,step_size,slice_num))

	def generate_static_document(self, data_source ='Changgung',subject_scan, atlas_name, feature_name, value):
		static_document=dict(data_source=data_source,scan=subject_scan,atlas=atlas_name,feature=feature_name,value=value,dynamic=0,comment='')
		return static_document

	def generate_dynamic_document(self, data_source='Changgung',subject_scan, atlas_name, feature_name, value, window_length, step_size, slice_num):
		dynamic_document=dict(data_source=data_source,scan=subject_scan,atlas=atlas_name,feature=feature_name,value=value,dynamic=1,window_length=window_length,step_size=step_size,slice=slice_num ,comment='')
		return dynamic_document


	def save_static_feature(self, feature):
		"""
		feature could be netattr.Net or netattr.Attr
		"""
		# check if feature already exist
		if self.exist_static(feature.scan, feature.atlasobj.name, feature.feature_name):
			raise MultipleRecordException(feature.scan, 'Please check again.')
		attrdata = pickle.dumps(feature.data)
		self.col = self.db['features']
		self.col.insert_one(self.generate_static_document(feature.scan, feature.atlasobj.name, feature.feature_name, attrdata))

	def remove_static_feature(self, scan, atlas_name, feature_name):
		self.col = self.db['features']
		self.col.find_one_and_delete(self.generate_static_query(scan, atlas_name, feature_name))

	def save_dynamic_attr(self,attr):
		"""
		attr class object: dynamic attr class obj
		example:
		data_source : Changgung;
		scan : CMSA_01;
		atlasobj_name: brodmann_lrce
		window_length : 100
		step_size : 3
		dynamic: 1
		slice: the num of slice
		feature/attr: ["inter-region_bc", "inter-region_ccfs","inter-region_wd","inter-region_le"]
		"""

		if self.exist_dynamic(attr.data_source,attr.scan ,attr.atlas_name,attr.feature_name,attr.window_length,attr.step_size):
			raise MultipleRecordException(attr.scan, 'Please check again.')
		self.col=self.db['dynamic_data']
		for i in range(attr.data.shape[1]):
			# i is the num of the column in data matrix
			data=attr.data[:,i]
			attr_value=pickle.dumps(data)
			slice_num = i
			self.col.insert_one(self.generate_dynamic_document(attr.data_source,attr.scan,attr.atlas_name,attr.feature_name,attr_value,attr.window_length,attr.step_size,slice_num))

	def	remove_dynamic_attr(self,scan,atlas_name,feature_name,dynamic_conf)
	#一些细节需要待定;整个删除；

			

	def get_atlasobj(self,atlas_name):
		return atlas.get(atlas_name)

	def get_attr(self, subject_scan, atlas_name, feature_name):
		#directly return to an attrobj
		if self.exist_static(subject_scan,atlas_name,feature_name):
			binary_data = self.query_static(subject_scan, atlas_name, feature_name)['value']
			attrdata = pickle.loads(binary_data)
			atlasobj = atlas.get(atlas_name)
			attr = netattr.Attr(attrdata, atlasobj, subject_scan, feature_name)
			return attr
		else:
			print("can't find the document you look for. scan: %s, atlas_name: %s, feature_name: %s." % (subject_scan, atlas_name, feature_name))
			return None

	def get_net(self, subject_scan, atlas_name, feature_name = 'BOLD.net'):
		if self.exist_static(subject_scan, atlas_name, feature_name = 'BOLD.net'):
			binary_data = self.query_static(subject_scan, atlas_name, feature_name = 'BOLD.net')['value']
			netdata = pickle.loads(binary_data)
			atlasobj = atlas.get(atlas_name)
			net = netattr.Net(netdata, atlasobj, subject_scan, 'BOLD.net')
			return net
		else:
			print("can't find the document you look for. scan: %s, atlas_name: %s, feature_name: %s." % (subject_scan, atlas_name, feature_name))
			raise NoRecordFoundException(subject_scan)
			return None

	def put_temp_data(self, temp_data, name, description = None):
		"""
		Insert temporary data into MongoDB. 
		Input temp_data as a serializable object (like np.array) and name as a string.
		The description argument is optional
		"""
		# check if name is already in temp database
		if self.temp_collection.count_documents(dict(name = name)) > 0:
			raise MultipleRecordException(name, 'Please consider a new name')
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
	def __init__(self, name, suggestion = ''):
		super(MultipleRecordException, self).__init__()
		self.name = name
		self.suggestion = suggestion

	def __str__(self):
		return 'Multiple record found for %s. %s' % (self.name, self.suggestion)

	def __repr__(self):
		return 'Multiple record found for %s. %s' % (self.name, self.suggestion)


class NoRecordFoundException(Exception):
	"""
	"""
	def __init__(self, name, suggestion = ''):
		super(NoRecordFoundException, self).__init__()
		self.name = name
		self.suggestion = ''

	def __str__(self):
		return 'No record found for %s. %s' % (self.name, self.suggestion)

	def __repr__(self):
		return 'No record found for %s. %s' % (self.name, self.suggestion)


if __name__ == '__main__':
	pass
