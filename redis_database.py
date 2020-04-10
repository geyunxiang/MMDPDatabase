# coding=utf-8
"""
Redis is a high-speed high-performance cache database.
A Redis database would be created on-the-fly and (possibly)
destroyed after usage.
"""
import mongodb_database
from redis import ConnectionPool, Redis
import os, sys
import pickle
import numpy as np
from mmdps.proc import netattr , atlas
from threading import Thread

class RedisDatabase:
	"""
	docstring for RedisDatabase
	"""

	def __init__(self, password = ""):
		self.start_redis()
		self.mdb = mongodb_database.MongoDBDatabase(password = password)

	def is_redis_running(self):
		if sys.platform == 'win32':
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
		elif sys.platform == 'darwin':
			# macOS
			pass
			return True
		else:
			# other platform
			return True

	def start_redis(self, password=''):
		if not self.is_redis_running():
			if sys.platform == 'win32':
				os.system("e:/redis/redis-server --service-start")
			elif sys.platform == 'darwin':
				# macOS
				pass
				raise Exception('redis-server not running.')
			else:
				pass
				raise Exception('redis-server not running.')
		try:
			self.data_pool = ConnectionPool(host='127.0.0.1', port=6379, db=0)
			self.cache_pool = ConnectionPool(host='127.0.0.1', port=6379, db=1, decode_responses=True)
			print('redis coonnection succecded')
		except Exception as e:
			print('redis connection failed，error message %s' % e)

	def stop_redis(self):
		if self.is_redis_running():
			# self.flushall()
			if sys.platform == 'win32':
				os.system("e:/redis/redis-server --service-stop")
			elif sys.platform == 'darwin':
				pass
			else:
				pass
		print("redis has been stopped")
	#待重写
	#set value
	def set_value(self,subject_scan, atlas_name, feature_name, isdynamic, value , window_length = 0 , step_size = 0, no = 1):
		rdb = Redis(connection_pool=self.data_pool)
		if isdynamic == False:
			rdb.set(self.generate_static_key(subject_scan, atlas_name, feature_name) , value, ex=1800)
		else:
			rdb.set(self.generate_dynamic_key(subject_scan, atlas_name, feature_name, window_length, step_size) + str(no), value, ex=1800)
		print('The key has been successfully inserted into redis')

	def generate_static_key(self, subject_scan, atlas_name, feature_name):
		key = subject_scan + ':' + atlas_name + ':' + feature_name + ':0'
		return key

	def generate_dynamic_key(self, subject_scan, atlas_name, feature_name, window_length, step_size):
		key = subject_scan + ':' + atlas_name + ':' + feature_name +':1:'+ str(window_length) + str(step_size)
		return key

	#get value
	def get_values(self, subject_scan, atlas_name = '', feature_name = '', isdynamic = False, window_length = 0, step_size = 0):
		#Invalid input check
		if type(atlas_name) is str and type(feature_name) is str and type(window_length) is int and type(step_size) is int:
			if isdynamic == False:
				res = self.get_static_value(subject_scan, atlas_name, feature_name)
			else:
				res = self.get_dynamic_value(subject_scan, atlas_name, feature_name, window_length, step_size)
			if res is None:
				return None # hot fix result not found error
			if len(res) == 1:
				return res[0]
			else:
				return res
		if type(subject_scan) is str:
			scan = []
			scan.append(subject_scan)
		else:
			scan = subject_scan
		if type(atlas_name) is str:
			atlas = []
			atlas.append(atlas_name)
		else:
			atlas = atlas_name
		if type(feature_name) is str:
			feature = []
			feature.append(feature_name)
		else:
			feature = feature_name
		if isdynamic == False:
			return self.get_static_values(scan, atlas, feature)
		else:
			#动态查询是否需要支持批量查询,暂时不支持
			return self.get_dynamic_values(scan, atlas, feature, window, step)

	def get_static_values(self, subject_scan, atlas_name, feature_name):
		lst=[]
		for i in subject_scan:
			for j in atlas_name:
				for k in feature_name:#pipeline改进
					lst.append(self.get_static_value(i,j,k))
		return lst

	def get_static_value(self, subject_scan, atlas_name, feature_name):
		rdb = Redis(connection_pool=self.data_pool)
		key=self.generate_static_key(subject_scan, atlas_name , feature_name)
		res=rdb.get(key)
		rdb.expire(key,1800)
		if not res:
			if self.mdb.exists_static(subject_scan, atlas_name, feature_name):
				doc=self.mdb.query_static(subject_scan, atlas_name , feature_name)
				rdb.set(self.generate_static_key(doc[0]['scan'],doc[0]['atlas'],doc[0]['feature']), (doc[0]['value']), ex=1800)
				return self.trans_netattr(doc[0]['scan'], doc[0]['atlas'], doc[0]['feature'],pickle.loads(doc[0]["value"]))
			else:
				print("Can't find the key: %s you look for" % key)
				return None
		else:
			return self.trans_netattr(subject_scan, atlas_name, feature_name,pickle.loads(res))

	def trans_netattr(self,subject_scan , atlas_name, feature_name, value):
		if feature_name not in ['dwi_net', 'bold_net']:  # 这里要改一下
			arr = netattr.Attr(value, atlas.get(atlas_name),subject_scan, feature_name)
			return arr
		else:
			net = netattr.Net(value, atlas.get(atlas_name), subject_scan)
			return net

	def get_dynamic_values(self,subject_scan, atlas_name, feature_name, window_length, step_size):
		lst = []
		for i in subject_scan:
			for j in atlas_name:
				for k in feature_name:#pipeline改进
					lst.append(self.get_dynamic_value(i, j, k ,window_length, step_size))
		return lst

	def get_dynamic_value(self, subject_scan, atlas_name, feature_name, window_length, step_size):
		rdb = Redis(connection_pool=self.data_pool)
		key_all = self.generate_dynamic_key(subject_scan, atlas_name, feature_name, window_length, step_size)
		if rdb.exists(key_all+':0'):
			rdb.expire(key_all+':0', 1800)
			pass
		else:
			if self.mdb.exists_dynamic(subject_scan, atlas_name, feature_name, window_length, step_size):
				doc = self.mdb.query_dynamic(subject_scan, atlas_name, feature_name, window_length, step_size)
				for j in doc: #使用查询关键字保证圣升序
					#pipeline改进
					rdb.set(self.generate_dynamic_key(j['scan'], j['atlas'], j['feature'], j['window length'], j['step size'],j["no"]), (j['value']), ex=1800)
					pass
			else:
				print("Can't find the key: %s you look for" % key)
				return None
		return lst
	#待重写
	def trans_dynamic_netattr(self, subject_scan, atlas_name, feature_name, window_length, step_size, value):
		if feature_name not in ['dwi_net', 'bold_net']:  # 这里要改一下
			arr = netattr.DynamicAttr(value, atlas.get(atlas_name), window_length, step_size, subject_scan, feature_name)
			return arr
		else:
			net = netattr.DynamicNet(value, atlas.get(atlas_name), window_length, step_size, subject_scan)
			return net

	def set_list_all_cache(self,key,value):
		rdb = Redis(connection_pool=self.cache_pool)
		rdb.delete(key)
		for i in value:
			rdb.rpush(key, i)
		#rdb.save()
		return rdb.llen(key)

	def set_list_cache(self,key,value):	#这里的key用list的名字之类的就可以，因为不是文件结构，所以键名不需要结构化
		rdb = Redis(connection_pool=self.cache_pool)
		rdb.rpush(key,value)
		#rdb.save()
		return rdb.llen(key)

	def get_list_cache(self, key, start = 0, end = -1):
		rdb = Redis(connection_pool=self.cache_pool)
		return rdb.lrange(key, start, end)

	def exists_key_cashe(self, key):
		rdb = Redis(connection_pool=self.cache_pool)
		return rdb.exists(key)

	def delete_key_cashe(self, key):
		rdb = Redis(connection_pool=self.cache_pool)
		value = rdb.delete(key)
		#rdb.save()
		return value

	def clear_cashe(self):
		rdb = Redis(connection_pool=self.cache_pool)
		rdb.flushdb()

	def flushall(self):
		rdb = Redis(connection_pool=self.data_pool)
		rdb.flushdb()
		rdb = Redis(connection_pool=self.cache_pool)
		rdb.flushdb()

if __name__ == '__main__':
	pass

'''-------------------------Version 2--------------------------------------
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
		lst = []
		for i in range(len(res)):
			query = keys[i].split(':')
			if not res[i]:
				if self.mdb.exists_static(query[0], query[1], query[2]):
					doc = self.mdb.query_static(query[0], query[1], query[2])
					for j in doc:
						self.rdb.set(self.generate_static_key(j["scan"],j["atlas"],j["feature"]), j["value"],ex=1800)
						lst.append(self.trans_netattr(j["scan"],j["atlas"],j["feature"],pickle.loads(j["value"])))
				else:
					print("Can't find the key: %s you look for "  % keys[i])
					continue
			else:
				lst.append(self.trans_netattr(query[0],query[1],query[2],pickle.loads(res[i])))
		return lst

	def expire_keys(self,keys):
		for key in keys:
			self.rdb.expire(key, 1800)

	---------------------------------------------------------------------------'''