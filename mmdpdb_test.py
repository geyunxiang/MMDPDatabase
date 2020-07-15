import numpy as np
import mmdpdb, mongodb_database
from mmdps.proc import loader, atlas
from mmdps.util import loadsave
import time,pymongo,pickle
import threading

SCAN =['baihanxiang_20190307','caipinrong_20180412','baihanxiang_20190211','caochangsheng_20161027',
       'caochangsheng_20161114','chenguwen_20150711','chenhua_20150711','chenyifan_20150612',
       'chenyifan_20150629','cuichenhao_20180704','cuichenhao_20180820',
       'cuichenhao_20180913','cuiwei_20150825','daihuachen_20170323','daihuachen_20170426',
       'daihuachen_20170518','daishiqin_20180521','daishiqin_20180705',
       'daizhongxi_20181116','denghongbin_20181117','denghongbin_20181203','dingshuqin_20180802',
       'fengdaoliang_20160107','fengdaoliang_20160120','fuchenhao_20170602','fuchenhao_20170623']
DTNAMIC_SCAN=['CMSA_01','CMSA_02']
ATLAS=['aal','aicha','bnatlas','brodmann_lr','brodmann_lrce']
ATTR_FEATURE=['BOLD.BC', 'BOLD.CCFS', 'BOLD.LE','BOLD.net']
FEATURE=['bold_interBC','bold_interCCFS','bold_interLE','bold_interWD','bold_net']
DTNAMIC_FEATURE=['bold_net','bold_net_attr']
WINDOW_LENTH=[22,50,100]
STEP_SIZE=[1,3]

def test_cache():
	db = mmdpdb.MMDPDatabase()
	# db.append_cache_list('test cache float', 1.123)
	# db.append_cache_list('test cache float', 2.234)
	lst = db.get_cache_list('test cache float')
	print(lst)

def test_get_dynamic_feature():
	a = mmdpdb.MMDPDatabase()
	a.get_dynamic_feature(DTNAMIC_SCAN,'brodmann_lrce','bold_net',22,1)

def insert_mongo(feat_name, dynamic_conf):
	atlasobj = atlas.get('brodmann_lrce')
	CMSA_dFeat = loader.load_dynamic_attr(loadsave.load_txt('/Users/andy/workshop/program_dynamic/CMSA_scan_list.txt'), atlasobj, 'inter-region_%s' % feat_name, dynamic_conf, rootFolder = '/Users/andy/workshop/MSADynamic/')
	NC_dFeat = loader.load_dynamic_attr(loadsave.load_txt('/Users/andy/workshop/program_dynamic/NC_scan_list.txt'), atlasobj, 'inter-region_%s' % feat_name, dynamic_conf, rootFolder = '/Users/andy/workshop/MSADynamic/')
	mdb = mongodb_database.MongoDBDatabase(data_source = 'MSA')
	for feat in CMSA_dFeat + NC_dFeat:
		mdb.save_dynamic_attr(feat)

def test_loader(feat_name, dynamic_conf):
	atlasobj = atlas.get('brodmann_lrce')
	CMSA_dFeat = loader.load_dynamic_attr(loadsave.load_txt('/Users/andy/workshop/program_dynamic/CMSA_scan_list.txt'), atlasobj, 'inter-region_%s' % feat_name, dynamic_conf, rootFolder = '/Users/andy/workshop/MSADynamic/')
	print(CMSA_dFeat[0].scan)
	print(CMSA_dFeat[0].data.shape)

def test_get_features():
	db = mmdpdb.MMDPDatabase(data_source = 'Changgung')
	feat = db.get_feature('chenyifan_20150629', 'brodmann_lrce', 'BOLD.BC')

	# mdb = mongodb_database.MongoDBDatabase(data_source = 'MSA')
	# feat = mdb.query_dynamic('MSA', 'CMSA_01', 'brodmann_lrce', 'BOLD.BC', 100, 3, slice_num = 1)

	print(feat.scan)
	print(feat.data.shape)

def compare_loader_database():
	db = mmdpdb.MMDPDatabase(data_source = 'Changgung')
	feat = db.get_feature('chenyifan_20150629', 'brodmann_lrce', 'BOLD.BC')
	feat_loader = loader.load_attrs(['chenyifan_20150629'], 'brodmann_lrce', 'BOLD.BC')[0]
	print(feat_loader.scan)
	print(feat_loader.data.shape)
	diff = np.abs(feat.data - feat_loader.data)
	print('maxdiff: ', np.max(diff))

def file_creater(size):
	myclient = pymongo.MongoClient("mongodb://localhost:27017/")
	mydb = myclient['test']
	mycol = mydb['dynamic_data']
	for i in range(size):
		a = np.random.rand(100,100)
		mydict = {'data_source' : 'test', 'scan': 'test_scan', 'atlas': 'aal', 'feature': 'test_feature', 'dynamic': 1,
                  'window_length': size, 'step_size': 1, 'slice_num': i, 'value': pickle.dumps(a)}
		mycol.insert_one(mydict)
		np.savetxt('../Feature/'+str(i)+'.csv',a)
def mmdpdb_speed_test(size):
	a = mmdpdb.MMDPDatabase('test')
	start = time.perf_counter()
	a.get_dynamic_feature('test_scan','aal','test_feature',size,1)
	end = time.perf_counter()
	print('mmdpdb running time is : %s s' %end-start)

b = mongodb_database.MongoDBDatabase('Changgung')
a = mmdpdb.MMDPDatabase()
class MyThread(threading.Thread):
	def __init__(self, scan):
		super(MyThread, self).__init__()
		self.scan = scan

	def run(self):
		x = a.get_feature(self.scan, 'aal', 'bold_interBC')
		y = b.query_static(self.scan, 'aal', 'bold_interBC')
		if (x.data - pickle.loads(y[0]['value'])).any():
			print(x.data)
			print(pickle.loads(y[0]['value']))
def thread_test():
	lst = []
	for i in SCAN:
		lst.append(MyThread('baihanxiang_20190307'))
	for i in lst:
		i.start()
	for i in lst:
		i.join()
class MyThread_dynamic(threading.Thread):
	def __init__(self, scan):
		super(MyThread_dynamic, self).__init__()
		self.scan = scan

	def run(self):
		x = a.get_dynamic_feature(self.scan, 'brodmann_lrce', 'bold_net',22,1)
		y = b.query_dynamic(self.scan,'brodmann_lrce','bold_net',22,1)
		z = []
		for i in y:
			z.append(pickle.loads(i['value']))
		z = np.array(z)
		z = z.swapaxes(0,2).swapaxes(0,1)
		if (x.data - z).any():
			print(x.data)
			print(z)
def thread_dynamic_test():
	lst = []
	for i in DTNAMIC_SCAN:
		lst.append(MyThread_dynamic('CMSA_01'))
	for i in lst:
		i.start()
	for i in lst:
		i.join()
if __name__ == '__main__':
	# test_cache()
	#a = mmdpdb.MMDPDatabase()
	#b = a.get_dynamic_feature(['123'],'123','123',1,'2')
	#print(b[0].data.shape)
	# insert_mongo('BC', (100, 3))
	# test_get_features()
	# test_loader('BC', (100, 3))
	a = mmdpdb.MMDPDatabase()
	print(a.get_feature('baihanxiang_20190307','brodmann_lrce','bold_net').data)
	#compare_loader_database()
	#thread_dynamic_test()
