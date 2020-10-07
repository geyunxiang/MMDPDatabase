"""
MongoDB test script goes here.
"""
import os
import time
import pickle
import json
import MongoDB as MDB
import numpy as np

from mmdps import rootconfig
from mmdps.proc import atlas, loader

atlas_list = ['brodmann_lr', 'brodmann_lrce',
              'aal', 'aicha', 'bnatlas']

DynamicAtlas = ['brodmann_lrce', 'aal']

attr_list = ['BOLD.BC.inter', 'BOLD.CCFS.inter',
             'BOLD.LE.inter', 'BOLD.WD.inter', 'BOLD.net']

dynamic_attr_list = ['inter-region_bc',
                     'inter-region_ccfs', 'inter-region_le', 'inter-region_wd']

DynamiConf = [[22, 1], [50, 1], [100, 1], [100, 3]]


def generate_static_database_attrs(data_source='Changgung'):
    mdb = MDB.MongoDBDatabase(data_source)
    mriscans = os.listdir(rootconfig.path.feature_root)

    for atlas_name in atlas_list:
        atlasobj = atlas.get(atlas_name)
        for attr_name in attr_list:
            if attr_name.find('net') != -1:
                continue
            for mriscan in mriscans:
                try:
                    attr = loader.load_attrs([mriscan], atlasobj, attr_name)
                    mdb.save_static_attr(attr[0])
                except OSError:
                    print('! not found! scan: %s, atlas: %s, attr: %s not found!' % (
                        mriscan, atlas_name, attr_name))
                except MDB.MultipleRecordException:
                    print(
                        '! Multiple record found ! scan: %s, atlas: %s, attr: %s' % (
                            mriscan, atlas_name, attr_name))
            mdb.createIndex('SA', atlas_name+':'+attr_name, ['scan'])


def generate_static_database_networks(data_source='Changgung'):
    """
    Generate MongoDB from scratch.
    Scan a directory and move the directory to MongoDB
    """
    mdb = MDB.MongoDBDatabase(data_source)
    mriscans = os.listdir(rootconfig.path.feature_root)
    for atlas_name in atlas_list:
        atlasobj = atlas.get(atlas_name)
        for mriscan in mriscans:
            try:
                net = loader.load_single_network(mriscan, atlasobj)
                mdb.save_static_net(net)
            except OSError:
                print('! not found! scan: %s, atlas: %s, network not found!' %
                      (mriscan, atlas_name))
            except MDB.MultipleRecordException:
                print(
                    '! Multiple record found ! scan: %s, atlas: %s ' % (
                        mriscan, atlas_name))
        mdb.createIndex('SN', atlas_name+':'+'BOLD.net', ['scan'])


def generate_dynamic_database_attrs(dynamic_rootfolder, data_source='Changgung'):
    mdb = MDB.MongoDBDatabase(data_source)
    mriscans = os.listdir(dynamic_rootfolder)

    for atlas_name in DynamicAtlas:
        atlasobj = atlas.get(atlas_name)
        for dynamic_conf in DynamiConf:
            for attrname in dynamic_attr_list:

                for mriscan in mriscans:
                    try:
                        attr = loader.load_single_dynamic_attr(
                            mriscan, atlasobj, attrname, dynamic_conf, dynamic_rootfolder)
                        mdb.save_dynamic_attr(attr)
                    except OSError:
                        print('! Not found! scan: %s, atlas: %s, attr: %s' %
                              (mriscan, atlas_name, attrname))
                    except MDB.MultipleRecordException:
                        print('! Mutiple found scan: %s,atlas: %s, attr: %s' %
                              (mriscan, atlas_name, attrname))
                col = mdb.getcol(atlas_name, attr.feature_name,
                                 dynamic_conf[0], dynamic_conf[1])
                mdb.createIndex('DA', col, ['scan', 'slice'])


def generate_dynamic_database_networks(dynamic_rootfolder, data_source='Changgung'):
    mdb = MDB.MongoDBDatabase(data_source)
    mriscans = os.listdir(dynamic_rootfolder)

    for atlas_name in atlas_list:
        atlasobj = atlas.get(atlas_name)
        for dynamic_conf in DynamiConf:
            for mriscan in mriscans:
                try:
                    net = loader.load_single_dynamic_network(
                        mriscan, atlasobj, dynamic_conf, dynamic_rootfolder)
                    mdb.save_dynamic_net(net)
                except OSError:
                    print('! Not found! scan: %s atlas: %s' %
                          (mriscan, atlas_name))
                except MDB.MultipleRecordException:
                    print('! Multiple found! scan: %s atlas:%s' %
                          (mriscan, atlas_name))
            col = mdb.getcol(atlas_name, 'BOLD.net',
                             dynamic_conf[0], dynamic_conf[1])
            mdb.createIndex('DN', col, ['scan', 'slice'])


"""
generate_static_database_attrs('Changgung')
generate_static_database_networks('Changgung')
generate_dynamic_database_attrs(rootconfig.path.dynamic_feature_root, 'MSA')
generate_dynamic_database_networks(rootconfig.path.dynamic_feature_root, 'MSA')
"""

rootfolder = 'C:\\Users\\THU-EE-WL\\Downloads\\MSA Dynamic Features'