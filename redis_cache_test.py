import time
import numpy as np
import redis_database

def traditional_way():
	lst_1 = np.zeros(30)
	lst_2 = np.zeros(30)
	for idx in range(lst_1.shape[0]):
		print('idx = %d' % idx)
		time.sleep(5)
		lst_1[idx] = idx
		lst_2[idx] = idx
	print(lst_1)
	print(lst_2)

def redis_similar_way():
	rdb = redis_database.RedisDatabase()
	exit()
	start_idx = 0
	if rdb.get_list_cache('test lst 1') is not None:
		pass
		start_idx = ...
	else:
		rdb.set_list_cache_all('test lst 1', [0]*30)
	for idx in range(start_idx, 30):
		print('idx = %d' % idx)
		time.sleep(5)
		rdb.set_list_at_idx('test lst 1', idx, idx)

def redis_way_1():
	rdb = redis_database.RedisDatabase()
	start_idx = 0
	
	lst_1 = rdb.get_list_cache('test lst 1')
	if rdb.get_list_cache('test lst 1') is not None:
		print('rdb cached %d element in lst 1' % len(lst_1))
		start_idx = len(lst_1)
		print(lst_1)

	for idx in range(start_idx, 30):
		print('idx = %d' % idx)
		time.sleep(5)
		rdb.set_list_cache('test lst 1', idx)
		rdb.set_list_cache('test lst 2', idx)
	lst_1 = rdb.get_list_cache('test lst 1')
	lst_2 = rdb.get_list_cache('test lst 2')
	print(lst_1)
	print(lst_2)
	rdb.clear_cache('test lst 1')
	rdb.clear_cache('test lst 2')

if __name__ == '__main__':
	redis_way_1()