"""
MongoDB test script goes here.
"""
import mongodb_database
import numpy as np
from mmdps import rootconfig

def main():
	mdb = mongodb_database.MongoDBDatabase(None)
	mat = np.array([[1, 2, 3], [4, 5, 6]])
	# mdb.remove_temp_data('test')
	# mdb.put_temp_data(mat, 'test')

	res = mdb.get_temp_data('test')
	print(res)

def test_loading():
	root_folder = rootconfig.path.feature_root
	mriscans = ['baihanxiang_20190307', 'caipinrong_20180412', 'baihanxiang_20190211']
	atlas_list = ['brodmann_lr', 'brodmann_lrce', 'aal', 'aicha', 'bnatlas']
	attr_list = ['BOLD.BC', 'BOLD.CCFS', 'BOLD.LE', 'BLD.WD', 'BOLD.net', 'DWI.FA', 'DWI.MD', 'DWI.net']
	"""
	atlasobj = atlas.get('brodmann_lrce')
	net = loader.load_single_network(atlasobj, 'baihanxiang_20190211')
	data_str = pickle.dumps(net.data)
	attr = loader.load_attrs(['baihanxiang_20190211'], atlasobj, 'BOLD.BC')
	#l=loader.AttrLoader(atlasobj,root_folder)
	#attr1=l.loadSingle('baihanxiang_20190211','BOLD.BC')
	#print(attr[0].data)
	#print(pickle.dumps(attr[0].data))
	"""

if __name__ == '__main__':
	main()
