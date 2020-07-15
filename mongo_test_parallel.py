import multiprocessing
import queue
import itertools
from functools import partial

import mongodb_database

from mmdps.proc import netattr
from mmdps.util import stats_utils

ASCI_control_list = ['C_10_liuyanan', 'C_11_baiyabin', 'C_12_yinjibing', 'C_13_zhouxiguo', 'C_14_songjie', 'C_15_lusanquan', 'C_16_zhangjunzhu', 'C_17_maodonghui', 'C_18_sunduanying', 'C_19_zhoucaixia', 'C_1_jingxiaoshan', 'C_20_shenaimin', 'C_21_wensufang', 'C_22_lulianling', 'C_23_wangyangxian', 'C_24_fengxiujuan', 'C_25_zhantiechao', 'C_26_zhaimaitao', 'C_27_fengyutong', 'C_28_liulu', 'C_29_caolaiwu', 'C_2_lidapeng', 'C_30_chenqingfeng', 'C_31_make', 'C_32_lizhanwei', 'C_33_yuyongtai', 'C_34_mengxiangshu', 'C_35_shimanhong', 'C_36_zhaojunmin', 'C_37_lanlihui', 'C_38_shaomingxiang', 'C_39_zhujunlu', 'C_3_zhangxin', 'C_40_changshoukun', 'C_41_qironghui', 'C_42_houqingdan', 'C_43_zhuhaonan', 'C_4_luozheng', 'C_5_mashurong', 'C_6_zhangshuhua', 'C_7_zhangjie', 'C_8_cuiyongdi', 'C_9_wangliping']

healthy_group_scanlist = ['chenguwen_20150711', 'chenhua_20150711', 'cuiwei_20150825', 'houxiuyun_20150712', 'huangyu_20150712', 'kangjian_20150826', 'linkuixing_20150712', 'linlikai_20150827', 'machunyue_20150711', 'qiuchunsuo_20150712', 'tuyuanyuan_20150827', 'wangdongxu_20150826', 'wuchaolan_20150711', 'wuqin_20150825', 'xiaoyanqing_20150827', 'xuquan_20150826', 'yangxiaohui_20150827', 'yanshuyu_20150712', 'yeshengnan_20150712', 'zhangjing_20150711', 'zhangli_20150711', 'zhengjia_20150825']

MSA_NC_scanlist = ['NC_01', 'NC_02', 'NC_03', 'NC_04', 'NC_05', 'NC_06', 'NC_07', 'NC_08', 'NC_09', 'NC_10', 'NC_11', 'NC_12', 'NC_13', 'NC_14', 'NC_15', 'NC_16', 'NC_17', 'NC_18', 'NC_19', 'NC_20']

def comparison(threshold_tuple_chunk, mdb_1_source, scanlist_1, mdb_2_source, scanlist_2, message_queue):
	mdb_1 = mongodb_database.MongoDBDatabase(mdb_1_source, host = '101.6.70.33', username = 'mmdpdb', password = '123.abc')
	mdb_2 = mongodb_database.MongoDBDatabase(mdb_2_source, host = '101.6.70.33', username = 'mmdpdb', password = '123.abc')
	for threshold_tuple in threshold_tuple_chunk:
		threshold_1, threshold_2 = threshold_tuple
		feature_list_1 = [mdb_1.get_attr(scan, 'brodmann_lrce', 'T1.GMD', {'threshold': threshold_1}) for scan in scanlist_1]
		feature_list_2 = [mdb_2.get_attr(scan, 'brodmann_lrce', 'T1.GMD', {'threshold': threshold_2}) for scan in scanlist_2]
		stat_attr, p_attr = netattr.attr_comparisons(feature_list_1, feature_list_2, stats_utils.twoSampleTTest)
		result_dict = dict(data_source_1 = mdb_1.data_source, threshold_1 = threshold_1, data_source_2 = mdb_2.data_source, threshold_2 = threshold_2, stat_data = stat_attr.data, p_data = p_attr.data, atlas = 'brodmann_lrce')
		message_queue.put(result_dict)
	return True

def err(arg):
	print(arg)

def chunk_list(sequence, chunk_num):
	chunk_size = int(len(sequence)/chunk_num)
	res = [None] * chunk_num
	for i in range(chunk_num):
		res[i] = sequence[i*chunk_size:(i+1)*chunk_size]
	res[-1] = sequence[(chunk_num - 1)*chunk_size:]
	return res

def parallel_test():
	num_worker = 4
	with multiprocessing.Pool(num_worker) as pool:
		manager = multiprocessing.Manager()
		message_queue = manager.Queue()
		threshold_tuple_list = itertools.product([threshold_1/100.0 for threshold_1 in range(100)], [threshold_2/100.0 for threshold_2 in range(100)])
		# threshold_tuple_list = itertools.product([threshold_1/100.0 for threshold_1 in range(5)], [threshold_2/100.0 for threshold_2 in range(5)])
		task_count = len(threshold_tuple_list)
		finished_count = 0
		part_compare = partial(comparison, message_queue = message_queue, mdb_1_source = 'Changgung_HC_T1_GMD', mdb_2_source = 'ASCI_T1_GMD', scanlist_1 = healthy_group_scanlist, scanlist_2 = ASCI_control_list)
		result = pool.map_async(part_compare, chunk_list(list(threshold_tuple_list), num_worker), error_callback = err)
		print('mapped')
		# while not result.ready():
		while finished_count < task_count:
			try:
				ret = message_queue.get(timeout = 2)
			except queue.Empty:
				continue
			else:
				finished_count += 1
				print('%d/%d (%1.3f), queue length: %d' % (finished_count, task_count, finished_count/float(task_count), message_queue.qsize()))
				print(ret['stat_data'].shape)
				# temp_data = dict(stat_data = ret.pop('stat_data'), p_data = ret.pop('p_data'))
				# mdb_local.put_temp_data(temp_data, ret, overwrite = True)
		result.get()
		print('finished')

if __name__ == '__main__':
	parallel_test()
