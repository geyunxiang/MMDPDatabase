import mmdpdb, mongodb_database
from mmdps.proc import loader, atlas
from mmdps.util import loadsave

SCAN =['baihanxiang_20190307','caipinrong_20180412','baihanxiang_20190211','caochangsheng_20161027',
       'caochangsheng_20161114','chenguwen_20150711','chenhua_20150711','chenyifan_20150612',
       'chenyifan_20150629','cuichenhao_20180704','cuichenhao_20180820',
       'cuichenhao_20180913','cuiwei_20150825','daihuachen_20170323','daihuachen_20170426',
       'daihuachen_20170518','daishiqin_20180521','daishiqin_20180705',
       'daizhongxi_20181116','denghongbin_20181117','denghongbin_20181203','dingshuqin_20180802',
       'fengdaoliang_20160107','fengdaoliang_20160120','fuchenhao_20170602','fuchenhao_20170623']
DTNAMIC_SCAN=['CMSA_01','CMSA_02','CMSA_03']
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
	db = mmdpdb.MMDPDatabase(data_source = 'MSA')
	feat = db.get_dynamic_feature('CMSA_01', 'brodmann_lrce', 'BOLD.BC', 100, 3, data_source = 'MSA')

	# mdb = mongodb_database.MongoDBDatabase(data_source = 'MSA')
	# feat = mdb.query_dynamic('MSA', 'CMSA_01', 'brodmann_lrce', 'BOLD.BC', 100, 3, slice_num = 1)

	print(feat.scan)
	print(feat.data.shape)

if __name__ == '__main__':
	# test_cache()
	# a = mmdpdb.MMDPDatabase()
	# a.get_dynamic_feature('CMSA_01','brodmann_lrce','bold_net',22,1)
	# insert_mongo('BC', (100, 3))
	test_get_features()
	test_loader('BC', (100, 3))
