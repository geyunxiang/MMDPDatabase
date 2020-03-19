# coding=utf-8
"""
Redis is a high-speed high-performance cache database.
A Redis database would be created on-the-fly and (possibly)
destroyed after usage.
"""
import mongodb_database
import redis
import os
import pickle
import numpy as np
from mmdps.proc import netattr , atlas
from threading import Thread

class RedisDatabase:
	"""
	docstring for RedisDatabase
	"""

	def __init__(self, password="" ):
		self.start_redis()
		# pool = redis.ConnectionPool()
		self.mdb= mongodb_database.MongoDBDatabase(password = password)

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

	def start_redis(self, password=''):
		if not self.is_redis_running():
			os.system("e:/redis/redis-server --service-start")
		try:
			#pool = redis.ConnectionPool(host,port)
			#self.r = redis.Redis(pool,db)
			self.rdb = redis.Redis(host='127.0.0.1', port=6379, db=1)
			print('redis coonnection succeded')
		except Exception as e:
			print('redis connection failed，error message %s' % e)

	def stop_redis(self):
		if self.is_redis_running():
			self.rdb.flushall()
			os.system("e:/redis/redis-server --service-stop")
		print("redis has been stopped")

	def generate_static_key(self, subject_scan, atlas_name, feature_name):
		key = subject_scan + ':' + atlas_name + ':' + feature_name +':0'
		return key

	def set_static_value(self,subject_scan , atlas_name , feature_name, value):
		self.rdb.set(self.generate_static_key(subject_scan, atlas_name , feature_name ), value,ex=1800)
		print('The key has been successfully inserted into redis')
#use
	def get_values(self, subject_scan , atlas_name = '*' , feature_name = '*', isdynamic = False, window_length = 0, step_size = 0):
		if isdynamic == False:
			if type(subject_scan) is str and type(atlas_name) is str and type(feature_name) is str:
				res = self.get_static_value(subject_scan, atlas_name, feature_name)
				if len(res) == 1:
					return res[0]
				else:
					return res
			else:
				if type(subject_scan) is str:
					scan = []
					scan.append(subject_scan)
				else:
					scan = subject_scan
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
				return self.get_static_values(scan, altas, feature)
		else:
			pass #dynamic data
	def get_static_values(self, subject_scan, atlas_name, feature_name):
		list=[]
		for i in subject_scan:
			for j in atlas_name:
				for k in feature_name:
					value = self.get_static_value(i,j,k)
					if value != None:
						list += value
		return list

	'''-------------------------Version 2--------------------------------------'''
	def get_static_values2(self, subject_scan, atlas_name, feature_name):
		keys=[]
		for i in subject_scan:
			for j in atlas_name:
				for k in feature_name:
					key = self.generate_static_key(i, j, k)
					keys.append(key)
		res = self.rdb.mget(keys)
		self.thread = Thread(target = self.expire_keys(keys))
		self.thread.start()
		list = []
		for i in range(len(res)):
			query = keys[i].split(':')
			if not res[i]:
				if self.mdb.exists_static(query[0], query[1], query[2]):
					doc = self.mdb.query_static(query[0], query[1], query[2])
					for j in doc:
						self.rdb.set(self.generate_static_key(j["scan"],j["atlas"],j["feature"]), j["value"],ex=1800)
						list.append(self.trans_netattr(j["scan"],j["atlas"],j["feature"],pickle.loads(j["value"])))
				else:
					print("Can't find the key: %s you look for "  % keys[i])
					continue
			else:
				list.append(self.trans_netattr(query[0],query[1],query[2],pickle.loads(res[i])))
		return list

	def expire_keys(self,keys):
		for key in keys:
			self.rdb.expire(key, 1800)
	'''---------------------------------------------------------------------------'''
	def get_static_value(self, subject_scan, atlas_name, feature_name):
		key=self.generate_static_key(subject_scan, atlas_name , feature_name)
		res=self.rdb.get(key)
		self.rdb.expire(key,1800)
		list=[]
		if not res:
			if self.mdb.exists_static(subject_scan, atlas_name, feature_name):
				doc=self.mdb.query_static(subject_scan, atlas_name , feature_name)
				for j in doc:
					self.rdb.set(self.generate_static_key(j['scan'],j['altas'],j['feature']), (j['value']), ex=1800)
					list.append(self.trans_netattr(j['scan'], j['atlas'], j['feature'],pickle.loads(j["value"])))
			else:
				print("Can't find the key: %s you look for" % self.generate_static_key(subject_scan, atlas_name, feature_name))
				return None
		else:
			list.append(self.trans_netattr(subject_scan, atlas_name, feature_name,pickle.loads(res)))
		return list

	def trans_netattr(self,subject_scan , atlas_name, feature_name, value):
		if feature_name not in ['dwi_net', 'bold_net']:  # 这里要改一下
			arr = netattr.Attr(value, atlas.get(atlas_name), feature_name)
			return arr
		else:
			net = netattr.Net(value, atlas.get(atlas_name), subject_scan)
			return net

	def flushall(self):
		self.rdb.flushall()

if __name__ == '__main__':
	pass
