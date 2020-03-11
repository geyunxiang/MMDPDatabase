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

class MongoDBDatabase:
	"""
	docstring for MongoDBDatabase
	"""
	def __init__(self,host, port, m_db, m_col):
		self.client = pymongo.MongoClient(host, port)
		self.db = self.client[m_db]
		self.col = self.db[m_col]

	def get_value(self, scanID, atlas_name, feature_name):
		self.scanID=scanID
		self.atlats_name=atlas_name
		self.feature_name=feature_name
		pass

	def set_value(self, scanID, atlas_name, feature_name, value):
		self.scanID=scanID
		self.atlats_name=atlas_name
		self.feature_name=feature_name
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
		return self.col.find(m_query)
	def exists_static(self,subject_name, scan_date, atlas_name , feature_name):
		return self.col.count_documents(self.generate_static_query(subject_name, scan_date, atlas_name , feature_name))

def generate_database(Directory_name):
	pass
def generate_database(host, port, m_db, m_col):
	"""
	Generate MongoDB from scratch. 
	Scan a directory and move the directory to MongoDB
	"""

def test_MongoDB_query():
	"""
	A test program.
	"""
	pass
def old_loading_method():
	"""
	Scan directory and find target. Uses mmdps.proc.loader
	"""
	from mmdps.proc import loader, atlas
	import pickle
	atlasobj = atlas.get('brodmann_lrce')
	net = loader.load_single_network(atlasobj, 'baihanxiang_20190211')
	print(net.data.shape)
	print(net.name)
	data_str = pickle.dumps(net.data)
	attr = loader.load_attrs(['baihanxiang_20190211'], atlasobj, 'BOLD.BC')
	print(attr[0].data.shape[1])
	print(attr[0].name)
	data_str = pickle.dumps(net.data)
if __name__ == '__main__':
	old_loading_method()