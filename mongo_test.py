"""
MongoDB test script goes here.
"""

import os
import sys
import mongodb_database
import numpy as np
import time

sys.path.append("C:\\Users\\THU-EE-WL\\Documents\\VScode Files\\mmdps")
from mmdps.proc import atlas, loader
from mmdps import rootconfig


atlas_list = ['brodmann_lr', 'brodmann_lrce', 'aal', 'aicha', 'bnatlas']
attr_list_full = ['BOLD.BC', 'BOLD.CCFS', 'BOLD.LE',
                  'BLD.WD', 'BOLD.net', 'DWI.FA', 'DWI.MD', 'DWI.net','DWI.MD','DWI.FA','DWI.net']
attr_list = ['BOLD.BC', 'BOLD.CCFS',
             'BOLD.LE', 'BOLD.WD', 'BOLD.net']
dynamic_attr_list = ['inter-region_bc',
                     'inter-region_ccfs', 'inter-region_le', 'inter-region_wd']
dynamic_conf_list = [[22, 1], [50, 1], [100, 1], [100, 3]]


def generate_static_database_attrs(data_source='Changgung'):
    """
    Generate MongoDB from scratch.
    Scan a directory and move the directory to MongoDB
    """
    database = mongodb_database.MongoDBDatabase(data_source)
    mriscans = os.listdir(rootconfig.path.feature_root)
    for mriscan in mriscans:
        for atlas_name in atlas_list:
            atlasobj = atlas.get(atlas_name)
            for attr_name in attr_list:
                if attr_name.find('net') != -1:
                    continue
                try:
                    attr = loader.load_attrs([mriscan], atlasobj, attr_name)
                    database.save_static_feature(attr[0])
                except OSError as e:
                    print('! not found! scan: %s, atlas: %s, attr: %s not found!' % (
                        mriscan, atlas_name, attr_name))


def generate_static_database_networks(data_source='Changgung'):
    """
    Generate MongoDB from scratch.
    Scan a directory and move the directory to MongoDB
    """
    database = mongodb_database.MongoDBDatabase(data_source)
    mriscans = os.listdir(rootconfig.path.feature_root)
    for mriscan in mriscans:
        for atlas_name in atlas_list:
            atlasobj = atlas.get(atlas_name)
            try:
                net = loader.load_single_network(mriscan, atlasobj)
                database.save_static_feature(net)
            except OSError as e:
                print('! not found! scan: %s, atlas: %s, network not found!' %
                      (mriscan, atlas_name))


def generate_dynamic_database_attrs(dynamic_rootfolder, data_soource='Changgung'):
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


def main():
    mdb = mongodb_database.MongoDBDatabase(None)
    mat = np.array([[1, 2, 3], [4, 5, 6]])
    # mdb.remove_temp_data('test')
    # mdb.put_temp_data(mat, 'test')

    res = mdb.get_temp_data('test')
    print(res)



data_source = 'Changgung'
start = time.time()
generate_static_database_attrs(data_source)
mdb=mongodb_database.MongoDBDatabase(data_source)
end = time.time()
mdb.colStats()
mdb.dbStats()
print("time",end-start)


if __name__ == '__main__':
    pass
