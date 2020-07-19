"""
MongoDB test script goes here.
"""

import os
import time
import pickle
import mongodb_database
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

""" Get attrname from csvfile name """
MappingDict = dict(zip(attr_name, attr_list))


def generate_static_database_attrs(data_source='Changgung'):
    """
    Generate MongoDB from scratch.
    Scan a directory and move the directory to MongoDB
    """
    mongodb_database = mongodb_database.MongoDBDatabase(data_source)
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
                except OSError as e:
                    print('! not found! scan: %s, atlas: %s, attr: %s not found!' % (
                        mriscan, atlas_name, attr_name))


def generate_static_database_networks(data_source='Changgung'):
    """
    Generate MongoDB from scratch.
    Scan a directory and move the directory to MongoDB
    """
    mdb = mongodb_database.MongoDBDatabase(data_source)
    mriscans = os.listdir(rootconfig.path.feature_root)
    for mriscan in mriscans:
        for atlas_name in atlas_list:
            atlasobj = atlas.get(atlas_name)
            try:
                net = loader.load_single_network(mriscan, atlasobj)
                mdb.save_static_feature(net)
            except OSError as e:
                print('! not found! scan: %s, atlas: %s, network not found!' %
                      (mriscan, atlas_name))


def generate_dynamic_database_attrs(dynamic_rootfolder, data_source='Changgung'):
    database = mongodb_database.MongoDBDatabase(data_source)
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
                except OSError as e:
                    print('! not found! scans: %s, attr: %s not found!' %
                          (mriscan, attrname))


def generate_dynamic_database_networks(dynamic_rootfolder, data_source='Changgung'):
    database = mongodb_database.MongoDBDatabase(data_source)
    mriscans = os.listdir(dynamic_rootfolder)
    atlas_name = 'brodmann_lrce'
    atlasobj = atlas.get(atlas_name)
    for mriscan in mriscans:
        for dynamic_conf in dynamic_conf_list:
            try:
                net = loader.load_single_dynamic_network(
                    mriscan, atlasobj, dynamic_conf, dynamic_rootfolder)
                database.save_dynamic_network(net)
            except OSError as e:
                print('! not found! scan: %s  not found!' % (mriscan))

    os.listdir(rootfolder)


def main():
    mdb = mongodb_database.MongoDBDatabase(None)
    mat = np.array([[1, 2, 3], [4, 5, 6]])
    # mdb.remove_temp_data('test')
    # mdb.put_temp_data(mat, 'test')

    res = mdb.get_temp_data('test')
    print(res)


def get_atlas_list(atlaspath, atlas_list):
    """ Get the intersection of two list"""
    atlas_folder = os.listdir(atlaspath)
    return list(set(atlas_folder).intersection(set(atlas_list)))


def get_attr_list(attrpath):
    """
    Attrpath is the csvfile path
    Get attr list through MappingDict
    """
    attr_names = os.listdir(attrpath)
    attrlist = []
    for attr_name in attr_names:
        attrlist.append(Mapping[attr_name])
    return attrlist


def test_load_feature(data_source='Changgung'):
    """ 
    Test time usage of mongo and loader 
    This function will check the folder's completeness
    """
    mdb = mongodb_database.MongoDBDatabase(data_source)
    loader_time = 0
    mongo_time = 0
    mriscans = os.listdir(rootconfig.path.feature_root)
    for mriscan in mriscans:
        atlas_path = os.path.join(rootconfig.path.feature_root, mriscan)
        for atlas_name in get_atlas_list(atlas_path, atlas_list):
            atlasobj = atlas.get(atlas_name)
            attr_path = os.path.join(atlas_path, atlas_name)
            for attr_name in get_attr_list(attr_path, attr_list):
                loader_start = time.time()
                attr = loader.load_attrs([mriscan], atlasobj, attr_name)
                loader_end = time.time()
                if mdb.exist_query('static', attr[0].scan, attr[0].atlasobj.name, attr[0].feature_name) != None:
                    print(attr[0].scan, 'Please check again.')
                else:
                    attrdata = pickle.dumps(attr[0].data)
                    document = mdb.get_document(
                        'static', attr[0].scan, attr[0].atlasobj.name, attr[0].feature_name, attrdata)
                    """"
                    documents=[]
                    documents.append(document)
                    mdb.db['features'].insert_many(documents)
                    """"
                    mongo_start = time.time()
                    mdb.db['features'].insert_one(document)
                    mongo_end = time.time()
                    loader_time += loader_end - loader_start
                    mongo_time += mongo_end - mongo_start
    print(loader_time)
    print(mongo_time)
    mdb.dbStats()
    mdb.colStats()


def check_all_feature(rootfolder, data_source='Changgung'):
    """
    Check all feature in rootfolder whether exist in mongo
    Get total query time and query speed
    """
    mdb = mongodb_database.MongoDBDatabase(data_source)
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


if __name__ == '__main__':
    pass
