"""
Redis is a high-speed high-performance cache database.
A Redis database would be created on-the-fly and (possibly)
destroyed after usage.
"""

import redis
import pymongo
class RedisDatabase:
	"""
	docstring for RedisDatabase
	"""

	#使用了三个变量好麻烦，有没有简化的方法？
	#我的设计是初始化的时候就连接上monggodb，因为不像redis在运行过程中monggodb不会关闭，所以一开始就连接上就行,不知道对不对？
	def __init__(self, client = "mongodb://localhost:27017/", db = "runoobdb", col = "static", password=""):
		self.rdb = redis.Redis()
		# pool = redis.ConnectionPool()
		self.mdb_client = pymongo.MongoClient(client)
		self.mdb = self.mdb_client[db]
		self.mdb_col = self.mdb[col]

	def is_redis_running(self):
		return True
		#会用命令行的话就会有系统问题

	def start_redis(self, host = '127.0.0.1', port = 6379, db=3, password=''):
		if not self.is_redis_running():
			pass
		try:
			#pool = redis.ConnectionPool(host,port)
			#self.r = redis.Redis(pool,db)
			self.rdb = redis.Redis(host, port, db)
			print('redis coonnection succeded')
		except Exception as e:
			print('redis connection failed，error message %s' % e)
		pass

	def get_data_from_mongodb(self, query = {'scan':'baihanxiang_20190307'}):
		doc = self.mdb_col.find(query)
		for i in doc:
			newkey = i['scan'] + ':' + i['atlas'] + ':' + i['feature'] + ':0'
			self.rdb.set(newkey, (i['content']))
		print("The keys have been successfully inserted into redis")

	def generate_key(self, subject_name = 'baihanxiang', scan_date = '20190307', atlas_name = 'aal', feature_name = 'bold_interBC', is_dynamic = False):
		key = subject_name+'_'+scan_date + ':' + atlas_name + ':' + feature_name + ':0'
		return key

	def set_value(self, key, value):
		self.rdb.set(key,value)
		print('The key has been successfully inserted into redis')

	def get_value(self, key):
		res = self.rdb.get(key)
		if res:
			return res.decode()
			#return pickle.loads(res.decode())
		else:
			list = key.split(':')
			query = {
				"scan": list[0],
				"atlas": list[1],
				"feature": list[2],
				"dynamic": 0
			}
			self.get_data_from_mongodb(query)
		#一个一个查询的话，效率会很低，如果设计成批量查询的会更好
		#同时，由于两个数据库的键值不是一种存储模式，有没有更好的解决的这个问题的方法

def test_generate_Redis():
	"""
	A test program that generates Redis database (possibly) based on directory.
	Note this function will not be used in the released version, since Redis will 
	query MongoDB to generate database rather than query directory. 
	"""
	pass

def test_Redis_query():
	"""
	A test program.
	"""
	pass

if __name__ == '__main__':
	a=RedisDatabase()
	a.start_redis()
	a.get_data_from_mongodb()
	print(a.get_value(a.generate_key()))
