"""
MongoDB test script goes here.
"""

import os
import time
import pickle
import json
import mongodb_database as MDB
import numpy as np

from mmdps import rootconfig
from mmdps.proc import atlas, loader


atlas_list = ['brodmann_lr', 'brodmann_lrce',
              'aal', 'aicha', 'bnatlas', 'aal2']

attr_list = ['BOLD.BC.inter', 'BOLD.CCFS.inter',
             'BOLD.LE.inter', 'BOLD.WD.inter', 'BOLD.net']

attr_name = ['bold_interBC.csv', 'bold_interCCFS.csv',
             'bold_interLE.csv', 'bold_interWD.csv', 'bold_net.csv']

dynamic_attr_list = ['inter-region_bc',
                     'inter-region_ccfs', 'inter-region_le', 'inter-region_wd']

attr_list_full = ['BOLD.BC.inter', 'BOLD.CCFS.inter', 'BOLD.LE.inter',
                  'BLD.WD.inter', 'BOLD.net.inter', 'DWI.FA', 'DWI.MD', 'DWI.net', 'DWI.MD', 'DWI.FA', 'DWI.net']

dynamic_conf_list = [[22, 1], [50, 1], [100, 1], [100, 3]]


with open("EEG_conf.json", 'r') as f:
    EEG_conf = json.loads(f.read())


def generate_static_database_attrs(data_source='Changgung'):
    """
    Generate MongoDB from scratch.
    Scan a directory and move the directory to MongoDB
    """
    mdb = MDB.MongoDBDatabase(data_source)
    mriscans = os.listdir(rootconfig.path.feature_root)
    for mriscan in mriscans:
        for atlas_name in atlas_list:
            atlasobj = atlas.get(atlas_name)
            for attr_name in attr_list:
                if attr_name.find('net') != -1:
                    continue
                try:
                    attr = loader.load_attrs([mriscan], atlasobj, attr_name)
                    mdb.save_static_feature(attr[0])
                except OSError:
                    print('! not found! scan: %s, atlas: %s, attr: %s not found!' % (
                        mriscan, atlas_name, attr_name))
                except MDB.MultipleRecordException:
                    print(
                        '! Multiple record found ! scan: %s, atlas: %s, attr: %s' % (
                            mriscan, atlas_name, attr_name))


def generate_static_database_networks(data_source='Changgung'):
    """
    Generate MongoDB from scratch.
    Scan a directory and move the directory to MongoDB
    """
    mdb = MDB.MongoDBDatabase(data_source)
    mriscans = os.listdir(rootconfig.path.feature_root)
    for mriscan in mriscans:
        for atlas_name in atlas_list:
            atlasobj = atlas.get(atlas_name)
            try:
                net = loader.load_single_network(mriscan, atlasobj)
                mdb.save_static_feature(net)
            except OSError:
                print('! not found! scan: %s, atlas: %s, network not found!' %
                      (mriscan, atlas_name))
            except MDB.MultipleRecordException:
                print(
                    '! Multiple record found ! scan: %s, atlas: %s ' % (
                        mriscan, atlas_name))


def generate_dynamic_database_attrs(dynamic_rootfolder, data_source='Changgung'):
    database = MDB.MongoDBDatabase(data_source)
    mriscans = os.listdir(dynamic_rootfolder)
    atlas_name = 'brodmann_lrce'
    atlasobj = atlas.get(atlas_name)
    for mriscan in mriscans:
        for dynamic_conf in dynamic_conf_list:
            for attrname in dynamic_attr_list:
                try:
                    attr = loader.load_single_dynamic_attr(
                        mriscan, atlasobj, attrname, dynamic_conf, dynamic_rootfolder)
                    database.save_dynamic_attr(attr)
                except OSError:
                    print('! not found! scans: %s, attr: %s not found!' %
                          (mriscan, attrname))
                except MDB.MultipleRecordException:
                    print('! Mutiple record found scan: %s, attr: %s' %
                          (mriscan, attrname))


def generate_dynamic_database_networks(dynamic_rootfolder, data_source='Changgung'):
    database = MDB.MongoDBDatabase(data_source)
    mriscans = os.listdir(dynamic_rootfolder)
    atlas_name = 'brodmann_lrce'
    atlasobj = atlas.get(atlas_name)
    for mriscan in mriscans:
        for dynamic_conf in dynamic_conf_list:
            try:
                net = loader.load_single_dynamic_network(
                    mriscan, atlasobj, dynamic_conf, dynamic_rootfolder)
                database.save_dynamic_network(net)
            except OSError:
                print('! not found! scan: %s  not found!' % (mriscan))
            except MDB.MultipleRecordException:
                print('! Multiple record found scan: %s' % (mriscan))


def generate_EEG_database(rootfolder, data_source='Changung'):
    mdb = MDB.MongoDBDatabase(data_source)
    mriscans = os.listdir(rootfolder)
    for mriscan in mriscans:
        path = os.path.join(rootfolder, mriscan)
        mats = os.listdir(path)
        for mat in mats:
            matpath = os.path.join(path, mat)
            datadict = mdb.loadmat(matpath)
            try:
                mdb.save_mat_dict(mriscan, mat, datadict)
            except MDB.MultipleRecordException:
                print('! Mutiple record found scan: %s, mat: %s ' %
                      (mriscan, mat))


def main():
    mdb = MDB.MongoDBDatabase(None)
    mat = np.array([[1, 2, 3], [4, 5, 6]])
    # mdb.remove_temp_data('test')
    # mdb.put_temp_data(mat, 'test')

    res = mdb.get_temp_data('test')
    print(res)


def test_load_feature(data_source='Changgung'):
    """
    Test time usage of mongo and loader
    This function will check the folder's completeness
    """
    MongoTime = 0
    LoaderTime = 0
    mdb = MDB.MongoDBDatabase(data_source)
    mriscans = os.listdir(rootconfig.path.feature_root)
    for mriscan in mriscans:
        for atlas_name in atlas_list:
            atlasobj = atlas.get(atlas_name)
            for attr_name in attr_list:
                if attr_name.find('net') != -1:
                    continue
                try:
                    loader_start = time.time()
                    attr = loader.load_attrs([mriscan], atlasobj, attr_name)
                    loader_end = time.time()
                    LoaderTime += loader_end-loader_start
                    mongo_time = mdb.save_static_feature(attr[0])
                except OSError:
                    print('! not found! scan: %s, atlas: %s, attr: %s not found!' % (
                        mriscan, atlas_name, attr_name))
                except MDB.MultipleRecordException:
                    print(
                        '! Multiple record found ! scan: %s, atlas: %s, attr: %s' % (
                            mriscan, atlas_name, attr_name))
                MongoTime += mongo_time
    print(LoaderTime)
    print(MongoTime)


def check_all_feature(rootfolder, data_source='Changgung'):
    """
    Check all feature in rootfolder whether exist in mongo
    Get total query time and query speed
    """
    mdb = MDB.MongoDBDatabase(data_source)
    mriscans = os.listdir(rootfolder)
    query_num = 0
    query_time = 0
    for mriscan in mriscans:
        for atlasname in atlas_list:
            for attrname in attr_list:
                query = dict(scan=mriscan, atlas=atlasname, attr=attrname)
                query_start = time.time()
                count = mdb.db['features'].find(query).count
                query_end = time.time()
                if count == 0:
                    print(mriscan, atlasname, attrname, "No record")
                elif count > 1:
                    print(mriscan, atalsname, attrname, "Repeated record")
                query_num += 1
                query_time += query_end-query_start
    print('Query Number', query_num)
    print('Query Time', query_time)


"""
mdb = mongodb_database.MongoDBDatabase('Changgung')
mriscan = 'EEG_feature_examples'
path = 'C:\\Users\\THU-EE-WL\\Desktop\\EEG_feature_examples'
mats = os.listdir(path)
for mat in mats:
    matpath = os.path.join(path, mat)
    datadict = mdb.loadmat(matpath)
    feature = EEG_conf[mat]['feature']
    dic = dict(scan=mriscan, feature=feature)
    if EEG_conf[mat]['fields'] == []:
        for k in datadict.keys():
            dic[feature] = pickle.dumps(datadict[k])
    else:
        for k in datadict.keys():
            DataArray = datadict[k]
            for field in EEG_conf[mat]['fields']:
                print(field)
                dic[field] = pickle.dumps(DataArray[field])
    mdb.db['EEG'].insert_one(dic)
"""


if __name__ == '__main__':
    pass
