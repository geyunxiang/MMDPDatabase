# coding=utf-8
"""
Redis is a high-speed high-performance cache database.
A Redis database would be created on-the-fly and (possibly)
destroyed after usage.
"""
from redis import ConnectionPool, StrictRedis
import os, sys
import pymongo
import pickle
import numpy as np
from mmdps.proc import netattr , atlas

class RedisDatabase:
	"""
	docstring for RedisDatabase
	"""

	def __init__(self, password = ""):
		self.start_redis()

	def is_redis_running(self):
		try:
			if sys.platform == 'win32':
				process = len(os.popen('tasklist | findstr ' + "redis-server.exe").readlines())
				if process >= 1:
					return True
				else:
					return False
			elif sys.platform == 'darwin':
				# macOS
				return True
			else:
				# other platform
				return True
		except Exception as e:
			raise Exception('Unable to check redis running staate，error message: ' + str(e))

	def start_redis(self, password=''):
		try:
			if not self.is_redis_running():
				if sys.platform == 'win32':
					os.system("e:/redis/redis-server --service-start")
				elif sys.platform == 'darwin':
					# macOS
					pass
				else:
					pass
		except Exception as e:
			raise Exception('Unble to start redis, error message: ' + str(e))
		try:
			self.datadb = StrictRedis(host='localhost', port=6379, db=0)
			self.cachedb = StrictRedis(host='localhost', port=6379, db=1, decode_responses=True)
			self.hashdb = StrictRedis(host='localhost', port=6379, db=2)
		except Exception as e:
			raise Exception('Redis connection failed，error message:' + str(e))

	def stop_redis(self):
		try:
			if self.is_redis_running():
				# self.flushall()
				if sys.platform == 'win32':
					os.system("e:/redis/redis-server --service-stop")
				elif sys.platform == 'darwin':
					pass
				else:
					pass
		except Exception as e:
			raise Exception('Unble to stop redis，error message:' + str(e))
	
	#set value
	def set_value(self, obj, data_source):
		if type(obj) is dict:
			key = self.generate_static_key(data_source, obj['scan'], obj['atlas'], obj['feature'])
			self.datadb.set(key, obj['value'], ex=1800)
			return self.trans_netattr(obj['scan'], obj['atlas'], obj['feature'], pickle.loads(obj['value']))
		elif type(obj) is pymongo.cursor.Cursor:
			len = 0
			value = []
			scan = obj[0]['scan']
			atlas = obj[0]['atlas']
			feature = obj[0]['feature']
			window_length = obj[0]['window_length']
			step_size = obj[0]['step_size']
			key_all = self.generate_dynamic_key(data_source, scan, atlas, feature, window_length, step_size)
			pipe = self.datadb.pipeline()
			len=obj.count()
			try:
				pipe.multi()
				pipe.set(key_all + ':0', len, ex=1600)
				for i in range(len):  # 使用查询关键字保证升序
					pipe.set(key_all + ':' + str(i+1), (obj[i]['value']), ex=1800)
					value.append(pickle.dumps(obj[i]['value']))
				pipe.execute()
			except Exception as e:
				raise Exception('An error occur when tring to set value in redis, error message: ' + str(e))
			return self.trans_dynamic_netattr(scan, atlas, feature, window_length, step_size, np.array(value))
		elif type(obj) is netattr.Net or type(obj) is netattr.Attr:
			key = self.generate_static_key(data_source, obj.scan, obj.atlasobj.name, obj.feature_name)
			self.atadb.set(key, pickle.dumps(obj.data))
		elif type(obj) is netattr.DynamicNet or type(obj) is netattr.DynamicAttr:
			key_all = self.generate_dynamic_key(data_source, obj.scan, obj.atlasobj.name, obj.feature_name, obj.window_length, obj.step_size)
			len=obj.data.shape[2]
			pipe = self.datadb.pipeline()
			if type(obj) is netattr.DynamicNet:
				flag = True
			else:
				flag = False
			try:
				pipe.multi()
				pipe.set(key_all + ':0', len, ex=1600)
				for i in range(len):  # 使用查询关键字保证升序
					if flag:
						pipe.set(key_all + ':' + str(i + 1), pickle.dumps(obj.data[:, :, i]), ex=1800)
					else:
						pipe.set(key_all + ':' + str(i + 1), obj.data[:, i], ex=1800)
				pipe.execute()
			except Exception as e:
				raise Exception('An error occur when tring to set value in redis, error message: ' + str(e))



	def generate_static_key(self, data_source, subject_scan, atlas_name, feature_name):
		key = data_source + ':' + subject_scan + ':' + atlas_name + ':' + feature_name + ':0'
		return key

	def generate_dynamic_key(self, data_source, subject_scan, atlas_name, feature_name, window_length, step_size):
		key = data_source + ':' + subject_scan + ':' + atlas_name + ':' + feature_name +':1:'+ str(window_length) + ':' + str(step_size)
		return key

	#get value
	def get_static_value(self, data_source, subject_scan, atlas_name, feature_name):
		key = self.generate_static_key(data_source, subject_scan, atlas_name, feature_name)
		res = self.datadb.get(key)
		self.datadb.expire(key, 1800)
		if res != None:
			return self.trans_netattr(subject_scan, atlas_name, feature_name, pickle.loads(res))
		else:
			return None

	def trans_netattr(self,subject_scan , atlas_name, feature_name, value):
		if feature_name not in ['dwi_net', 'bold_net']:  # 这里要改一下
			arr = netattr.Attr(value, atlas.get(atlas_name),subject_scan, feature_name)
			return arr
		else:
			net = netattr.Net(value, atlas.get(atlas_name), subject_scan, feature_name)
			return net

	def get_dynamic_value(self, data_source, subject_scan, atlas_name, feature_name, window_length, step_size):
		key_all = self.generate_dynamic_key(data_source, subject_scan, atlas_name, feature_name, window_length, step_size)
		if self.datadb.exists(key_all + ':0'):
			pipe = self.datadb.pipeline()
			try:
				pipe.multi()
				len = int(self.datadb.get(key_all + ':0').decode())
				for i in range(1,len + 1,1):
					pipe.get(key_all + ':' + str(i))
				res = pipe.execute()
			except Exception as e:
				raise Exception('An error occur when tring to get value in redis, error message: ' + str(e))
			try:
				pipe.multi()
				value = []
				for i in range(len):
					value.append(pickle.loads(res[i]))
					pipe.expire(key_all + ':' + str(i+1), 1800)
				pipe.expire(key_all + ':0', 1600)
				pipe.execute()
			except Exception as e:
				raise Exception('An error occur when tring to update expiration time in redis, error message: ' + str(e))
			return self.trans_dynamic_netattr(subject_scan, atlas_name, feature_name, window_length, step_size, np.array(value))
		else:
			return None
	
	def trans_dynamic_netattr(self, subject_scan, atlas_name, feature_name, window_length, step_size, value):
		if feature_name not in ['dwi_net', 'bold_net']:  # 这里要改一下
			arr = netattr.DynamicAttr(value.swapaxes(0,1), atlas.get(atlas_name), window_length, step_size, subject_scan, feature_name)
			return arr
		else:
			net = netattr.DynamicNet(value.swapaxes(0,2).swapaxes(0,1), atlas.get(atlas_name), window_length, step_size, subject_scan, feature_name)
			return net

	#is exist
	def exists_key(self,data_source, subject_scan, atlas_name, feature_name, isdynamic = False, window_length = 0, step_size = 0):
		if isdynamic ==False:
			return self.datadb.exists(self.generate_static_key(data_source, subject_scan, atlas_name, feature_name))
		else:
			return self.datadb.exists(self.generate_dynamic_key(data_source, subject_scan, atlas_name, feature_name, window_length, step_size) + ':0')

	def set_list_all_cache(self,key,value):
		self.cachedb.delete(key)
		for i in value:
			self.cachedb.rpush(key, i)
		#self.cachedb.save()
		return self.cachedb.llen(key)

	def set_list_cache(self,key,value):
		self.cachedb.rpush(key,value)
		#self.cachedb.save()
		return self.cachedb.llen(key)

	def get_list_cache(self, key, start = 0, end = -1):
		res = self.cachedb.lrange(key, start, end)
		lst=[]
		for x in res:
			if x.isdigit():
				lst.append(int(x))
			else:
				lst.append(float(x))
		return lst

	def exists_key_cache(self, key):
		return self.cachedb.exists(key)

	def delete_key_cache(self, key):
		value = self.cachedb.delete(key)
		#self.cachedb.save()
		return value

	def clear_cache(self):
		self.cachedb.flushdb()

	def set_hash_all(self,name,hash):
		self.hashdb.delete(name)
		for i in hash:
			hash[i]=pickle.dumps(hash[i])
		self.hashdb.hmset(name,hash)

	def set_hash(self,name, item1, item2=''):
		if type(item1) is dict:
			for i in item1:
				item1[i] = pickle.dumps(item1[i])
			self.hashdb.hmset(name,item1)
		else:
			self.hashdb.hset(name, item1, pickle.dumps(item2))
	def get_hash(self,name,keys=[]):
		if not keys:
			res = self.hashdb.hgetall(name)
			hash={}
			for i in res:
				hash[i.decode()]=pickle.loads(res[i])
			return hash
		else:
			if type(keys) is list:
				res = self.hashdb.hmget(name, keys)
				for i in range(len(res)):
					res[i]=pickle.loads(res[i])
				return res
			else:
				return pickle.loads(self.hashdb.hget(name, keys))

	def exists_hash(self,name):
		return self.hashdb.exists(name)

	def exists_hash_key(self,name,key):
		return self.hashdb.hexists(name, key)

	def delete_hash(self,name):
		self.hashdb.delete(name)

	def delete_hash_key(self,name,key):
		self.hashdb.hdel(name,key)

	def clear_hash(self):
		self.hashdb.flushdb()

	def flushall(self):
		self.datadb.flushall()
if __name__ == '__main__':
	pass
	#get value
