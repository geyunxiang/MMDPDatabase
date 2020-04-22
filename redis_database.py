# coding=utf-8
"""
Redis is a high-speed high-performance cache database.
A Redis database would be created on-the-fly and (possibly)
destroyed after usage.
"""
from redis import ConnectionPool, Redis
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
	
	#set value
	def set_value(self, obj):
		rdb = Redis(connection_pool=self.data_pool)
		if type(obj) is dict:
			key = self.generate_static_key(obj['scan'], obj['atlas'], obj['feature'])
			rdb.set(key, obj['value'], ex=1800)
			return self.trans_netattr(obj['scan'], obj['atlas'], obj['feature'], pickle.loads(obj['value']))
		elif type(obj) is pymongo.cursor.Cursor:
			len = 0
			value = []
			scan = obj[0]['scan']
			atlas = obj[0]['atlas']
			feature = obj[0]['feature']
			window_length = obj[0]['window length']
			step_size = obj[0]['step size']
			key_all = self.generate_dynamic_key(scan, atlas, feature, window_length, step_size)
			pipe = rdb.pipeline()
			try:
				pipe.multi()
				for j in obj:  # 使用查询关键字保证升序
					len = len + 1
					pipe.set(key_all + ':' + str(len), (j['value']), ex=1800)
					value.append(pickle.loads(j['value']))
				pipe.set(key_all + ':0', len, ex=1600)
				pipe.execute()
			except Exception as e:
				return e
			return self.trans_dynamic_netattr(scan, atlas, feature, window_length, step_size, np.array(value))
		elif type(obj) is netattr.Net:
			key = self.generate_static_key(obj.name, obj.atlasobj.name, 'bold_net')
			rdb.set(key, pickle.dumps(obj.data))
		elif type(obj) is netattr.Attr:
			key = self.generate_static_key(obj.name, obj.atlasobj.name, obj.attr_name)
			rdb.set(key, pickle.dumps(obj.data))
		elif type(obj) is netattr.DynamicNet:
			key = self.generate_dynamic_key(obj.name, obj.atlasobj.name, 'bold_net', obj.windowLength, obj.stepSize)
			rdb.set(key, pickle.dumps(obj.data))
		elif type(obj) is netattr.DynamicAttr:
			key = self.generate_dynamic_key(obj.name, obj.atlasobj.name, obj.attr_name, obj.windowLength, obj.stepSize)
			rdb.set(key, pickle.dumps(obj.data))

	def generate_static_key(self, subject_scan, atlas_name, feature_name):
		key = subject_scan + ':' + atlas_name + ':' + feature_name + ':0'
		return key

	def generate_dynamic_key(self, subject_scan, atlas_name, feature_name, window_length, step_size):
		key = subject_scan + ':' + atlas_name + ':' + feature_name +':1:'+ str(window_length) + ':' + str(step_size)
		return key

	#get value
	def get_static_value(self, subject_scan, atlas_name, feature_name):
		rdb = Redis(connection_pool=self.data_pool)
		key = self.generate_static_key(subject_scan, atlas_name, feature_name)
		res = rdb.get(key)
		rdb.expire(key, 1800)
		if res != None:
			return self.trans_netattr(subject_scan, atlas_name, feature_name, pickle.loads(res))
		else:
			return None

	def trans_netattr(self,subject_scan , atlas_name, feature_name, value):
		if feature_name not in ['dwi_net', 'bold_net']:  # 这里要改一下
			arr = netattr.Attr(value, atlas.get(atlas_name),subject_scan, feature_name)
			return arr
		else:
			net = netattr.Net(value, atlas.get(atlas_name), subject_scan)
			return net

	def get_dynamic_value(self, subject_scan, atlas_name, feature_name, window_length, step_size):
		rdb = Redis(connection_pool=self.data_pool)
		key_all = self.generate_dynamic_key(subject_scan, atlas_name, feature_name, window_length, step_size)
		if rdb.exists(key_all + ':0'):
			pipe = rdb.pipeline()
			try:
				pipe.multi()
				len = int(rdb.get(key_all + ':0').decode())
				for i in range(1,len + 1,1):
					pipe.get(key_all + ':' + str(i))
				res = pipe.execute()
			except Exception as e:
				return e
			try:
				pipe.multi()
				value = []
				for i in range(len):
					value.append(pickle.loads(res[i]))
					pipe.expire(key_all + ':' + str(i+1), 1800)
				pipe.expire(key_all + ':0', 1600)
				pipe.execute()
			except Exception as e:
				return e
			return self.trans_dynamic_netattr(subject_scan, atlas_name, feature_name, window_length, step_size, np.array(value))
		else:
			return None
	
	def trans_dynamic_netattr(self, subject_scan, atlas_name, feature_name, window_length, step_size, value):
		if feature_name not in ['dwi_net', 'bold_net']:  # 这里要改一下
			arr = netattr.DynamicAttr(value, atlas.get(atlas_name), window_length, step_size, subject_scan, feature_name)
			return arr
		else:
			net = netattr.DynamicNet(value, atlas.get(atlas_name), window_length, step_size, subject_scan)
			return net

	#is exist
	def exists_key(self, subject_scan, atlas_name, feature_name, isdynamic = False, window_length = 0, step_size = 0):
		rdb = Redis(connection_pool=self.data_pool)
		if isdynamic ==False:
			return rdb.exists(self.generate_static_key(subject_scan, atlas_name, feature_name))
		else:
			return rdb.exists(self.generate_dynamic_key(subject_scan, atlas_name, feature_name, window_length, step_size) + ':0')

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
	#get value
	'''
	def get_values(self, subject_scan, atlas_name = '', feature_name = '', isdynamic = False, window_length = 0, step_size = 0):
		#Invalid input check
		if type(atlas_name) is str and type(feature_name) is str and type(subject_scan) is str:
			if isdynamic == False:
				res = self.get_static_value(subject_scan, atlas_name, feature_name)
			else:
				res = self.get_dynamic_value(subject_scan, atlas_name, feature_name, window_length, step_size)
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
			return self.get_dynamic_values(scan, atlas, feature, window_length, step_size)
		'''
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