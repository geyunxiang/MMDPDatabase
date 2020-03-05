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

class MongoDBDatabase:
	"""
	docstring for MongoDBDatabase
	"""
	def __init__(self):
		pass

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

def generate_database(Directory_name):
	"""
	Generate MongoDB from scratch. 
	Scan a directory and move the directory to MongoDB
	"""
	import pymongo
	client=pymongo.MongoClient(host='localhost',port=27017)
	db=client['CSV-data']
	filename=Directory_name
	pass

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
	print(attr[0].data.shape)
	print(attr[0].name)
	data_str = pickle.dumps(net.data)

if __name__ == '__main__':
	old_loading_method()
