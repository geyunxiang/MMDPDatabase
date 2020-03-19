"""
MongoDB test script goes here.
"""
import mongodb_database
import numpy as np

def main():
	mdb = mongodb_database.MongoDBDatabase(None)
	mat = np.array([[1, 2, 3], [4, 5, 6]])
	# mdb.remove_temp_data('test')
	# mdb.put_temp_data(mat, 'test')

	res = mdb.get_temp_data('test')
	print(res)

if __name__ == '__main__':
	main()
