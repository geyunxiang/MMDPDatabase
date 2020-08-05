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

DynamicAtlas = ['brodmann_lrce', 'aal']

attr_list = ['BOLD.BC.inter', 'BOLD.CCFS.inter',
             'BOLD.LE.inter', 'BOLD.WD.inter', 'BOLD.net']

dynamic_attr_list = ['inter-region_bc',
                     'inter-region_ccfs', 'inter-region_le', 'inter-region_wd']

attr_list_full = ['BOLD.BC.inter', 'BOLD.CCFS.inter', 'BOLD.LE.inter',
                  'BLD.WD.inter', 'BOLD.net.inter', 'DWI.FA', 'DWI.MD', 'DWI.net', 'DWI.MD', 'DWI.FA', 'DWI.net']

DynamiConf = [[22, 1], [50, 1], [100, 1], [100, 3]]


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
    for mriscan in mriscans:
        for atlas_name in DynamicAtlas:
            atlasobj = atlas.get(atlas_name)
            for dynamic_conf in DynamiConf:
                for attrname in dynamic_attr_list:
                    try:
                        attr = loader.load_single_dynamic_attr(
                            mriscan, atlasobj, attrname, dynamic_conf, dynamic_rootfolder)
                        database.save_dynamic_attr(attr)
                    except OSError:
                        print('! Not found! scan: %s, atlas: %s, attr: %s' %
                              (mriscan, atlas_name, attrname))
                    except MDB.MultipleRecordException:
                        print('! Mutiple found scan: %s,atlas: %s, attr: %s' %
                              (mriscan, atlas_name, attrname))


def generate_dynamic_database_networks(dynamic_rootfolder, data_source='Changgung'):
    database = MDB.MongoDBDatabase(data_source)
    mriscans = os.listdir(dynamic_rootfolder)
    atlas_name = 'brodmann_lrce'
    atlasobj = atlas.get(atlas_name)
    for mriscan in mriscans:
        for dynamic_conf in DynamiConf:
            try:
                net = loader.load_single_dynamic_network(
                    mriscan, atlasobj, dynamic_conf, dynamic_rootfolder)
                database.save_dynamic_network(net)
            except OSError:
                print('! Not found! scan: %s atlas: %s' %
                      (mriscan, atlas_name))
            except MDB.MultipleRecordException:
                print('! Multiple found! scan: %s atlas:%s' %
                      (mriscan, atlas_name))


def generate_EEG_database(rootfolder, data_source='Changgung'):
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
                print('! Mutiple record found ! scan: %s, mat: %s' %
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
    MongoTime = LoaderTime = 0
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
                    if mdb.exist_query('static', attr[0].scan, attr[0].atlasobj.name, attr[0].feature_name) != None:
                        raise MDB.MultipleRecordException(
                            attr[0].scan, 'Please check again.')
                    doc = mdb.get_document(
                        'static', attr[0].scan, attr[0].atlasobj.name, attr[0].feature_name, pickle.dumps(attr[0].data))
                    mongo_start = time.time()
                    mdb.db['features'].insert_one(doc)
                    mongo_end = time.time()
                    MongoTime += mongo_end-mongo_start
                except OSError:
                    print('! not found! scan: %s, atlas: %s, attr: %s not found!' % (
                        mriscan, atlas_name, attr_name))
                except MDB.MultipleRecordException:
                    print(
                        '! Multiple record found ! scan: %s, atlas: %s, attr: %s' % (
                            mriscan, atlas_name, attr_name))
    print(LoaderTime)
    print(MongoTime)


def check_all_feature(rootfolder, data_source='Changgung'):
    """
    Check all feature in rootfolder whether exist in mongo
    Get total query time and query speed
    """
    mdb = MDB.MongoDBDatabase(data_source)
    mriscans = os.listdir(rootfolder)
    query_total_num = query_find_num = 0
    query_time = 0
    for mriscan in mriscans:
        for atlasname in atlas_list:
            for attrname in attr_list:
                query = dict(scan=mriscan, atlas=atlasname, feature=attrname)
                query_start = time.time()
                count = mdb.db['features'].count_documents(query)
                query_end = time.time()
                if count == 0:
                    print(mriscan, atlasname, attrname, "No record")
                elif count > 1:
                    print(mriscan, atlasname, attrname, "Repeated record")
                else:
                    print(mriscan, atlasname, attrname, "Normal record")
                    query_find_num += 1
                query_total_num += 1
                query_time += query_end-query_start
    print('Query Total Number', query_total_num)
    print("Query Find  Number", query_find_num)
    print('Query Time', query_time)


def LoadDynamicAttrTest(rootfolder, data_source='Changgung'):
    loadertime = 0
    mriscans = os.listdir(rootfolder)
    for atlas_name in DynamicAtlas:
        atlasobj = atlas.get(atlas_name)
        for attrname in dynamic_attr_list:
            for dynamic_conf in DynamiConf:
                try:
                    loadstart = time.time()
                    loader.load_dynamic_attr(
                        mriscans, atlasobj, attrname, dynamic_conf, rootfolder)
                    loadend = time.time()
                except OSError:
                    print('oserror')
                loadertime += loadend - loadstart
    print("Loader Time ", loadertime)


def LoadDynamicNetTest(rootfolder, data_source='Changgung'):
    loadertime = 0
    mriscans = os.listdir(rootfolder)
    for mriscan in mriscans:
        for atlas_name in DynamicAtlas:
            atlasobj = atlas.get(atlas_name)
            for dynamic_conf in DynamiConf:
                try:
                    loadstart = time.time()
                    loader.load_single_dynamic_network(
                        mriscan, atlasobj, dynamic_conf, rootfolder)
                    loadend = time.time()
                except OSError:
                    print('oserror')
                loadertime += loadend-loadstart
    print("Loader Time ", loadertime)


def DynamicAttrTest(rootfolder, data_source='Changgung'):
    """ Query and return dynamic attr object test"""
    mdb = MDB.MongoDBDatabase(data_source)
    QueryTime = QueryCount = 0
    mriscans = os.listdir(rootfolder)
    for mriscan in mriscans:
        for atlas_name in DynamicAtlas:
            for dynamic_conf in DynamiConf:
                for attrname in dynamic_attr_list:
                    try:
                        query_start = time.time()
                        mdb.get_dynamic_attr(
                            mriscan, atlas_name, attrname, dynamic_conf[0], dynamic_conf[1])
                        query_end = time.time()
                        QueryTime += query_end - query_start
                        QueryCount += 1
                    except MDB.NoRecordFoundException:
                        print('! Not found! scan: %s, atlas: %s, attr: %s' %
                              (mriscan, atlas_name, attrname))
    print("query time", QueryTime)
    print("query count", QueryCount)


def DynamicNetTest(rootfolder, data_source='Changgung'):
    """ Query and return dynamic net object test"""
    mdb = MDB.MongoDBDatabase(data_source)
    QueryTime = QueryCount = 0
    mriscans = os.listdir(rootfolder)
    for mriscan in mriscans:
        for atlas_name in DynamicAtlas:
            for dynamic_conf in DynamiConf:
                try:
                    query_start = time.time()
                    mdb.get_dynamic_net(
                        mriscan, atlas_name, dynamic_conf[0], dynamic_conf[1])
                    query_end = time.time()
                    QueryTime += query_end - query_start
                    QueryCount += 1
                except MDB.NoRecordFoundException:
                    print('! Not found! scan: %s, atlas: %s' %
                          (mriscan, atlas_name))
    print("query time", QueryTime)
    print("query count", QueryCount)


def test_load_dynamic_networks(dynamic_rootfolder, data_source='MSA'):
    """
    """
    database = MDB.MongoDBDatabase(data_source)
    mriscans = os.listdir(dynamic_rootfolder)
    atlas_name = 'brodmann_lrce'
    atlasobj = atlas.get(atlas_name)
    query_start = time.time()
    for mriscan in mriscans:
        try:
            net = loader.load_single_dynamic_network(
                mriscan, atlasobj, (100, 3), dynamic_rootfolder)
        except OSError:
            print('! not found! scan: %s  not found!' % (mriscan))
    query_end = time.time()
    print('Loader query time: %1.3fs' % (query_end - query_start))
    query_start = time.time()
    for mriscan in mriscans:
        try:
            net = database.get_dynamic_net(mriscan, atlasobj.name, 100, 3)
        except MDB.NoRecordFoundException:
            print('! not found! scan: %s  not found!' % (mriscan))
    query_end = time.time()
    print('Mongo query time: %1.3fs' % (query_end - query_start))


if __name__ == '__main__':
    dynamic_rootfolder = "C:\\Users\\THU-EE-WL\\Downloads\\MSA Dynamic Features"
    rootfolder = "C:\\Users\\THU-EE-WL\\Desktop\\EEG"
    """
    generate_EEG_database(rootfolder)
    generate_dynamic_database_attrs(dynamic_rootfolder)
    """
    generate_dynamic_database_networks(dynamic_rootfolder)
