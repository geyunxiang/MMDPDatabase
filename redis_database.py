# coding=utf-8
"""
Redis is a high-speed high-performance cache database.
A Redis database would be created on-the-fly and (possibly)
destroyed after usage.
"""
import mongodb_database
import redis
import pymongo
import os
import pickle
import re
import numpy as np
from mmdps.proc import netattr , atlas

class RedisDatabase:
	"""
	docstring for RedisDatabase
	"""

	def __init__(self, host = 'localhost', port = 27017, db = "runoobdb", col = "static", password=""):
		self.rdb = redis.Redis()
		# pool = redis.ConnectionPool()
		#self.mdb_client = pymongo.MongoClient(client)
		#self.mdb = self.mdb_client[db]
		#self.mdb_col = self.mdb[col]
		self.mdb= mongodb_database.MongoDBDatabase(host,port,db,col)

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

	def generate_database(self, subject_name = '', scan_date = '' , atlas_name = '' , feature_name = ''):
		if self.mdb.exists_static(subject_name, scan_date , atlas_name , feature_name):
			doc = self.mdb.query_static(subject_name, scan_date , atlas_name , feature_name)
			for i in doc:
				newkey = i['scan'] + ':' + i['atlas'] + ':' + i['feature'] + ':0'
				self.rdb.set(newkey, (i['content']),ex=1800)
			print("The keys have been successfully inserted into redis")
		else:
			print("Can't find the key you look for")

	def generate_static_key(self, subject_name, scan_date, atlas_name, feature_name):
		key = subject_name+'_'+scan_date + ':' + atlas_name + ':' + feature_name +':0'
		return key

	def set_static_value(self,subject_name , scan_date , atlas_name , feature_name, value):
		self.rdb.set(self.generate_static_key(subject_name , scan_date , atlas_name , feature_name ), value,ex=1800)
		print('The key has been successfully inserted into redis')

	def get_static_values(self, subject_name , scan_date , atlas_name , feature_name):
		subject =[]
		if type(subject_name) is str:
			subject = []
			subject.append(subject_name)
		else:
			subject = subject_name
		if type(scan_date) is str:
			scan = []
			scan.append(scan_date)
		else:
			scan=scan_date
		if type(atlas_name) is str:
			altas = []
			altas.append(atlas_name)
		else:
			altas = atlas_name
		if type(feature_name) is str:
			feature = []
			feature.append(feature_name)
		else:
			feature = feature_name
		keys=[]
		for i in subject:
			for j in scan:
				for k in altas:
					for l in feature:
						key = self.generate_static_key(i,j,k,l)
						keys.append(key)
		res = self.rdb.mget(keys)
		list=[]
		for i in range(len(res)):
			if not res[i]:
				query = re.split(':|_',keys[i])
				if self.mdb.exists_static(query[0], query[1], query[2], query[3]):
					doc = self.mdb.query_static(query[0], query[1], query[2], query[3])
					value=pickle.loads(doc[0]["content"])
					self.rdb.set(keys[i], doc[0]["content"])
				else:
					print("Can't find the key you look for")
					continue
			else:
				value = pickle.loads(res[i])
			if  keys[i].split(':')[2] not in ['dwi_net','bold_net']: #这里要改一下
				arr = netattr.Attr(value, atlas.get(atlas_name), keys[i].split(':')[2])
				list.append(arr)
			else:
				net = netattr.Net(value, atlas.get(atlas_name), keys[i].split(':')[0])
				list.append(net)
		return list

	def flushall(self):
		self.rdb.flushall()

	def get_static_value(self, subject_name , scan_date , atlas_name , feature_name):
		key=self.generate_static_key(subject_name , scan_date , atlas_name , feature_name)
		res=self.rdb.get(key)
		if not res:
			if self.mdb.exists_static(subject_name, scan_date, atlas_name, feature_name):
				doc=self.mdb.query_static(subject_name , scan_date , atlas_name , feature_name)
				res = doc[0]["content"]
				self.rdb.set(key,res)
			else:
				print("Can't find the key you look for")
				return None
		value = pickle.loads(res)
		if  feature_name not in ['dwi_net','bold_net']:
			arr = netattr.Attr(value,atlas.get(atlas_name),feature_name)
			return arr
		else:
			net = netattr.Net(value,atlas.get(atlas_name),feature_name) #这个命名规则我没有看懂
			return net

if __name__ == '__main__':
	a=RedisDatabase()
	a.start_redis()
	a.generate_database()
	b = a.get_static_values('baihanxiang',['20190307','20190307'],'aal','bold_net')
	print(b[0].data.shape)
	print(b[0].name)
	print(b[1].data.shape)
	print(b[1].name)
