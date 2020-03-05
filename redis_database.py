"""
Redis is a high-speed high-performance cache database.
A Redis database would be created on-the-fly and (possibly)
destroyed after usage.
"""


import redis
import pymongo
import os

class RedisDatabase:
	"""
	docstring for RedisDatabase
	"""

	def __init__(self, client = "mongodb://localhost:27017/", db = "runoobdb", col = "static", password=""):
		self.rdb = redis.Redis()
		# pool = redis.ConnectionPool()
		#链接这里会有改动
		self.mdb_client = pymongo.MongoClient(client)
		self.mdb = self.mdb_client[db]
		self.mdb_col = self.mdb[col]

	#我在考虑下面三个代码的实现有没有必要
	def is_redis_running(self):
		try:
			process = len(os.popen('tasklist | findstr ' + "redis-server.exe").readlines())
			print('redis state : %s' % process)
			if process >= 1:
				return True
			else:
				return False
		except Exception as e:
			print('redis connection failed，error message %s' % e)
			return False

	def start_redis(self, host = '127.0.0.1', port = 6379, db=1, password=''):
		if not self.is_redis_running():
			os.system("e:/redis/redis-server --service-start")
		try:
			#pool = redis.ConnectionPool(host,port)
			#self.r = redis.Redis(pool,db)
			self.rdb = redis.Redis(host, port, db)
			print('redis coonnection succeded')
		except Exception as e:
			print('redis connection failed，error message %s' % e)

	def stop_redis(self):
		if self.is_redis_running():
			self.rdb.flushall()
			os.system("e:/redis/redis-server --service-stop")
		print("redis has been stopped")

	#这个接口可能会用monggodb的接口
	def get_data_from_mongodb(self, query = {'scan':'caipinron_20180412'}):
		doc = self.mdb_col.find(query)
		if doc != None:
			for i in doc:
				newkey = i['scan'] + ':' + i['atlas'] + ':' + i['feature'] + ':0'
				self.rdb.set(newkey, (i['content']))
			print("The keys have been successfully inserted into redis")
		else:
			print("Can't find the key you look for")

	def generate_key(self, subject_name = 'baihanxiang', scan_date = '20190307', atlas_name = 'aal', feature_name = 'bold_interBC', is_dynamic = False):
		if is_dynamic == False:
			key = subject_name+'_'+scan_date + ':' + atlas_name + ':' + feature_name + ':0'
		else:
			pass #这里写动态数据
		return key

	#重载了赋值接口(这好像不是重载）
	def set_value_byname(self,subject_name, scan_date, atlas_name, feature_name, is_dynamic, value):
		key=self.generate_key(subject_name, scan_date, atlas_name, feature_name, is_dynamic)
		self.set_value_bykey(key,value)
	def set_value_bykey(self, key, value):
		self.rdb.set(key,value)
		print('The key has been successfully inserted into redis')

	#重载了查询接口，查询接口在MongoDB那里面
	def get_value_byname(self,subject_name, scan_date, atlas_name, feature_name, is_dynamic):
		key=self.generate_key(subject_name,scan_date,atlas_name,feature_name,is_dynamic)
		self.get_value_bykey(key)
	def get_value_bykey(self, key):
		res = self.rdb.get(key)
		if res:
			return res.decode()
			#return pickle.loads(res.decode()) #使用pickle储存的返回情况
		else:
			list = key.split(':')
			query = {
				"scan": list[0],
				"atlas": list[1],
				"feature": list[2],
				"dynamic": 0
			}
			self.get_data_from_mongodb(query)
	def flushall(self):
		self.rdb.flushall()

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
	a.flushall()
	a.get_data_from_mongodb()
	print(a.get_value(a.generate_key()))

