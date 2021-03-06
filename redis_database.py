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

	def __init__(self, expire_time = 1800):
		self.expire_time = max(expire_time, 1800)
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
			self.cachedb = StrictRedis(host='localhost', port=6379, db=1)
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

	def set_value(self, obj, data_source, atlas, feature, window_length=0, step_size=0):
		"""
		Using a dictionary, a Mongdb object, a Net class, a Attr class, a DynamicNet class or a DynamicAttr class
			to set a new entry in Redis.
		"""
		if type(obj) is dict:
			key = self.generate_static_key(data_source, obj['scan'], atlas, feature, obj['comment'])
			self.datadb.set(key, obj['value'], ex=self.expire_time)
			return self.trans_netattr(obj['scan'], atlas, feature, pickle.loads(obj['value']))
		elif type(obj) is list:
			value = []
			scan = obj[0]['scan']
			comment = obj[0]['comment']
			key_all = self.generate_dynamic_key(data_source, scan, atlas, feature, window_length, step_size, comment)
			pipe = self.datadb.pipeline()
			length = len(obj)
			try:
				pipe.multi()
				pipe.set(key_all + ':0', length, ex=self.expire_time - 200)
				for i in range(length):  # 使用查询关键字保证升序
					pipe.set(key_all + ':' + str(i + 1), (obj[i]['value']), ex=self.expire_time)
					value.append(pickle.loads(obj[i]['value']))
				pipe.execute()
			except Exception as e:
				raise Exception('An error occur when tring to set value in redis, error message: ' + str(e))
			return self.trans_dynamic_netattr(scan, atlas, feature, window_length, step_size, np.array(value))
		elif type(obj) is netattr.Net or type(obj) is netattr.Attr:
			key = self.generate_static_key(data_source, obj.scan, obj.atlasobj.name, obj.feature_name, {})
			self.datadb.set(key, pickle.dumps(obj.data))
		elif type(obj) is netattr.DynamicNet or type(obj) is netattr.DynamicAttr:
			key_all = self.generate_dynamic_key(data_source, obj.scan, obj.atlasobj.name, obj.feature_name, obj.window_length, obj.step_size, {})
			length=obj.data.shape[2]
			pipe = self.datadb.pipeline()
			if type(obj) is netattr.DynamicNet:
				flag = True
			else:
				flag = False
			try:
				pipe.multi()
				pipe.set(key_all + ':0', length, ex=self.expire_time - 200)
				for i in range(length):  # 使用查询关键字保证升序
					if flag:
						pipe.set(key_all + ':' + str(i + 1), pickle.dumps(obj.data[:, :, i]), ex=self.expire_time)
					else:
						pipe.set(key_all + ':' + str(i + 1), obj.data[:, i], ex=self.expire_time)
				pipe.execute()
			except Exception as e:
				raise Exception('An error occur when tring to set value in redis, error message: ' + str(e))



	def generate_static_key(self, data_source, subject_scan, atlas_name, feature_name, comment):
		key = data_source + ':' + subject_scan + ':' + atlas_name + ':' + feature_name + ':0'
		if comment != None:
			key += ':' + str(comment)
		return key

	def generate_dynamic_key(self, data_source, subject_scan, atlas_name, feature_name, window_length, step_size, comment):
		key = data_source + ':' + subject_scan + ':' + atlas_name + ':' + feature_name +':1:'+ str(window_length) + ':' + str(step_size)
		if comment != None:
			key += ':' + str(comment)
		return key

	def get_static_value(self, data_source, subject_scan, atlas_name, feature_name, comment = {}):
		"""
		Using data source, scan name, altasobj name, feature name to query static networks and attributes from Redis.
		If the query succeeds, return a Net or Attr class, if not, return none.
		"""
		key = self.generate_static_key(data_source, subject_scan, atlas_name, feature_name, comment)
		res = self.datadb.get(key)
		self.datadb.expire(key, self.expire_time)
		if res is not None:
			return self.trans_netattr(subject_scan, atlas_name, feature_name, pickle.loads(res))
		else:
			return None

	def trans_netattr(self,subject_scan, atlas_name, feature_name, value):
		if value.ndim == 1:  # 这里要改一下
			arr = netattr.Attr(value, atlas.get(atlas_name),subject_scan, feature_name)
			return arr
		else:
			net = netattr.Net(value, atlas.get(atlas_name), subject_scan, feature_name)
			return net

	def get_dynamic_value(self, data_source, subject_scan, atlas_name, feature_name, window_length, step_size, comment = {}):
		"""
		Using data source, scan name, altasobj name, feature name, window length, step size to query dynamic
			networks and attributes from Redis.
		If the query succeeds, return a DynamicNet or DynamicAttr class, if not, return none.
		"""
		key_all = self.generate_dynamic_key(data_source, subject_scan, atlas_name, feature_name, window_length, step_size, comment)
		if self.datadb.exists(key_all + ':0'):
			pipe = self.datadb.pipeline()
			try:
				pipe.multi()
				length = int(self.datadb.get(key_all + ':0').decode())
				for i in range(1,length + 1,1):
					pipe.get(key_all + ':' + str(i))
				res = pipe.execute()
			except Exception as e:
				raise Exception('An error occur when tring to get value in redis, error message: ' + str(e))
			try:
				pipe.multi()
				value = []
				for i in range(length):
					value.append(pickle.loads(res[i]))
					pipe.expire(key_all + ':' + str(i+1), self.expire_time)
				pipe.expire(key_all + ':0', self.expire_time - 200)
				pipe.execute()
			except Exception as e:
				raise Exception('An error occur when tring to update expiration time in redis, error message: ' + str(e))
			return self.trans_dynamic_netattr(subject_scan, atlas_name, feature_name, window_length, step_size, np.array(value))
		else:
			return None

	def trans_dynamic_netattr(self, subject_scan, atlas_name, feature_name, window_length, step_size, value):
		if value.ndim == 2:  # 这里要改一下
			arr = netattr.DynamicAttr(value.swapaxes(0,1), atlas.get(atlas_name), window_length, step_size, subject_scan, feature_name)
			return arr
		else:
			net = netattr.DynamicNet(value.swapaxes(0,2).swapaxes(0,1), atlas.get(atlas_name), window_length, step_size, subject_scan, feature_name)
			return net

	def exists_key(self,data_source, subject_scan, atlas_name, feature_name, isdynamic = False, window_length = 0, step_size = 0, comment ={}):
		"""
		Using data source, scan name, atlas name, feature name to check the existence of an static entry in Redis.
		You can add isdynamic(True), window length, step size to check the existence of an dynamic entry in Redis.
		"""
		if isdynamic is False:
			return self.datadb.exists(self.generate_static_key(data_source, subject_scan, atlas_name, feature_name,comment))
		else:
			return self.datadb.exists(self.generate_dynamic_key(data_source, subject_scan, atlas_name, feature_name, window_length, step_size, comment) + ':0')

	"""
	Redis supports storing and querying list as cache.
	Note: the items in list must be int or float.
	"""

	def set_list_all_cache(self,key,value):
		"""
		Store a list to Redis as cache with cache_key.
		Note: please check the existence of the cache_key, or it will cover the origin entry.
		"""
		self.cachedb.delete(key)
		for i in value:
			self.cachedb.rpush(key, pickle.dumps(i))
		#self.cachedb.save()
		return self.cachedb.llen(key)

	def set_list_cache(self,key,value):
		"""
		Append value to a list as the last one in Redis with cache_key.
		If the given key is empty in Redis, a new list will be created.
		"""
		self.cachedb.rpush(key, pickle.dumps(value))
		#self.cachedb.save()
		return self.cachedb.llen(key)

	def get_list_cache(self, key, start = 0, end = -1):
		"""
		Return a list with given cache_key in Redis.
		"""
		res = self.cachedb.lrange(key, start, end)
		lst=[]
		for x in res:
				lst.append(pickle.loads(x))
		return lst

	def exists_key_cache(self, key):
		"""
		Check the existence of a list in Redis by cache_key.
		"""
		return self.cachedb.exists(key)


	def delete_key_cache(self, key):
		"""
		Delete an entry in Redis by cache_key.
		If the given key is empty in Redis, do nothing.
		"""
		value = self.cachedb.delete(key)
		#self.cachedb.save()
		return value


	def clear_cache(self):
		"""
		Delete all the entries in Redis.
		"""
		self.cachedb.flushdb()

	"""
	Redis supports storing and querying hash.
	Note: the keys in hash must be string.
	"""

	def set_hash_all(self,name,hash):
		"""
		Store a hash to Redis with hash_name and a hash.
		Note: please check the existence of the hash_name, or it will cover the origin hash.
		"""
		self.hashdb.delete(name)
		for i in hash:
			hash[i]=pickle.dumps(hash[i])
		self.hashdb.hmset(name,hash)


	def set_hash(self,name, item1, item2=''):
		"""
		Append an entry/entries to a hash in Redis with hash_name.
		If the given name is empty in Redis, a new hash will be created.
		The input format should be as follows:
			1.A hash
			2.A key and a value
		"""
		if type(item1) is dict:
			for i in item1:
				item1[i] = pickle.dumps(item1[i])
			self.hashdb.hmset(name,item1)
		else:
			self.hashdb.hset(name, item1, pickle.dumps(item2))

	def get_hash(self,name,keys=[]):
		"""
		Support three query functions:
			1.Return a hash with a given hash_name in Redis.
			2.Return a value_list with a given hash_name and a key_list in Redis,
				the value_list is the same sequence as key_list.
			3.Return a value with a given hash_name and a key in Redis.
		"""
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
		"""
		Check the existence of a hash in Redis by hash_name.
		"""
		return self.hashdb.exists(name)


	def exists_hash_key(self,name,key):
		"""
		Check the existence of a key in a given hash by key_name and hash_name.
		"""
		return self.hashdb.hexists(name, key)


	def delete_hash(self,name):
		"""
		Delete a hash in Redis by hash_name.
		"""
		self.hashdb.delete(name)


	def delete_hash_key(self,name,key):
		"""
		Delete a key in a given hash by key_name and hash_name.
		"""
		self.hashdb.hdel(name,key)


	def clear_hash(self):
		"""
		Delete all the hashes in Redis by hash_name.
		"""
		self.hashdb.flushdb()

	def flushall(self):
		self.datadb.flushall()

if __name__ == '__main__':
	pass
	#get value
