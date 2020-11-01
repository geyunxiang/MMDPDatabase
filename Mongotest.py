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

DynamicConf = [[22, 1], [50, 1], [100, 1], [100, 3]]


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
            mdb.createIndex('SA', atlas_name+'-'+attr_name, ['scan'])


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
        mdb.createIndex('SN', atlas_name+'-'+'BOLD.net', ['scan'])


def generate_dynamic_database_attrs(dynamic_rootfolder, data_source='Changgung'):
    mdb = MDB.MongoDBDatabase(data_source)
    mriscans = os.listdir(dynamic_rootfolder)

    for atlas_name in DynamicAtlas:
        atlasobj = atlas.get(atlas_name)
        for dynamic_conf in DynamicConf:
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

    atlas_name = 'brodmann_lrce'
    atlasobj = atlas.get(atlas_name)
    for dynamic_conf in DynamicConf:
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


def test_load_static_attrs(feature_root=rootconfig.path.feature_root, data_source='Changgung'):
    """
    Test query time of loader and mongo when loading static attrs
    All scans x atlas x attrs are loaded
    """
    mdb = MDB.MongoDBDatabase(data_source)
    mriscans = list(os.listdir(feature_root))

    load_counter = 0
    query_start = time.time()
    for mriscan in mriscans:
        for atlas_name in atlas_list:
            atlasobj = atlas.get(atlas_name)
            for attr_name in attr_list:
                if attr_name.find('net') != -1:
                    continue
                try:
                    attr = loader.load_attrs([mriscan], atlasobj, attr_name)
                    load_counter += 1
                except OSError:
                    pass
                    #print('! not found! scan: %s, atlas: %s, attr: %s not found!' % (mriscan, atlas_name, attr_name))
    query_end = time.time()
    query_time = query_end - query_start
    print('Query %d static attrs (netattr.Attr) using loader time cost: %1.2fs' % (
        load_counter, query_time))

    """
    load_counter = 0
    query_start = time.time()
    for mriscan in mriscans:
        for atlas_name in atlas_list:
            for attr_name in attr_list:
                if attr_name.find('net') != -1:
                    continue
                try:
                    attr = mdb.get_static_attr(mriscan, atlas_name, attr_name)
                    load_counter += 1
                except MDB.NoRecordFoundException:
                    pass
                    # print('! not found! scan: %s, atlas: %s, attr: %s not found!' % (mriscan, atlas_name, attr_name))
    query_end = time.time()
    query_time = query_end - query_start
    print('Query %d static attrs (netattr.Attr) using MongoDB time cost: %1.2fs' % (
        load_counter, query_time))
    print(attr.data.shape)
    """


def test_load_static_networks(feature_root=rootconfig.path.feature_root, data_source='Changgung'):
    """
    Test query time of loader and mongo when loading static networks
    All scans x atlas are loaded
    """
    mdb = MDB.MongoDBDatabase(data_source)
    mriscans = list(os.listdir(feature_root))

    load_counter = 0
    query_start = time.time()
    for mriscan in mriscans:
        for atlas_name in atlas_list:
            atlasobj = atlas.get(atlas_name)
            try:
                net = loader.load_single_network(mriscan, atlasobj)
                load_counter += 1
            except OSError:
                pass
                # print('! not found! scan: %s, atlas: %s, network not found!' % (mriscan, atlas_name))
    query_end = time.time()
    query_time = query_end - query_start
    print('Query %d static networks (netattr.Net) using loader time cost: %1.2fs' % (
        load_counter, query_time))

    """
    load_counter = 0
    query_start = time.time()
    for mriscan in mriscans:
        for atlas_name in atlas_list:
            try:
                net = mdb.get_static_net(mriscan, atlas_name)
                load_counter += 1
            except MDB.NoRecordFoundException:
                pass
                # print('! not found! scan: %s, atlas: %s, network not found!' % (mriscan, atlas_name))
    query_end = time.time()
    query_time = query_end - query_start
    print('Query %d static networks (netattr.Net) using MongoDB time cost: %1.2fs' % (
        load_counter, query_time))
    print(net.data.shape)
    """


def test_load_dynamic_attrs(dynamic_rootfolder=rootconfig.path.dynamic_feature_root, data_source='Changgung'):
    """
    Test query time of loader and mongo when loading dynamic attrs
    All scans of a specific atlas and attr are loaded
    """
    database = MDB.MongoDBDatabase(data_source)
    mriscans = list(os.listdir(dynamic_rootfolder))
    atlas_name = 'brodmann_lrce'
    attr_name = 'inter-region_bc'
    atlasobj = atlas.get(atlas_name)

    load_counter = 0
    query_start = time.time()
    try:
        attr = loader.load_dynamic_attr(
            mriscans, atlasobj, attr_name, (22, 1), dynamic_rootfolder)
        load_counter += len(mriscans)
    except OSError:
        pass
        # print('! not found! scan: %s  not found!' % (attr_name))
    query_end = time.time()
    print('Query %d dynamic attrs (netattr.DynamicAttr) using loader time cost: %1.2fs' % (
        load_counter, query_end - query_start))

    load_counter = 0
    query_start = time.time()
    for mriscan in mriscans:
        try:
            attr = database.get_dynamic_attr(
                mriscan, atlas_name, 'BOLD.BC.inter', 22, 1)
            load_counter += 1
        except MDB.NoRecordFoundException:
            pass
            # print('! not found! scan: %s  not found!' % (mriscan))
    query_end = time.time()
    print('Query %d dynamic attrs (netattr.DynamicAttr) using MongoDB time cost: %1.2fs' % (
        load_counter, query_end - query_start))
    print(attr.data.shape)


def test_load_dynamic_networks(dynamic_rootfolder=rootconfig.path.dynamic_feature_root, data_source='Changgung'):
    """
    Test query time of loader and mongo when loading dynamic networks
    All scans of a specific atlas are loaded
    """
    database = MDB.MongoDBDatabase(data_source)
    mriscans = list(os.listdir(dynamic_rootfolder))
    atlas_name = 'brodmann_lrce'
    atlasobj = atlas.get(atlas_name)

    load_counter = 0
    query_start = time.time()
    for mriscan in mriscans:
        try:
            net = loader.load_single_dynamic_network(
                mriscan, atlasobj, (22, 1), dynamic_rootfolder)
            load_counter += 1
        except OSError:
            pass
            # print('! not found! scan: %s  not found!' % (mriscan))
    query_end = time.time()
    print('Query %d dynamic networks (netattr.DynamicNet) using loader time cost: %1.2fs' % (
        load_counter, query_end - query_start))

    load_counter = 0
    query_start = time.time()
    for mriscan in mriscans:
        try:
            net = database.get_dynamic_net(mriscan, atlasobj.name, 22, 1)
            load_counter += 1
        except MDB.NoRecordFoundException:
            pass
            # print('! not found! scan: %s  not found!' % (mriscan))
    query_end = time.time()
    print('Query %d dynamic networks (netattr.DynamicNet) using MongoDB time cost: %1.2fs' % (
        load_counter, query_end - query_start))
    print(net.data.shape)


if __name__ == '__main__':
    rootfolder = 'C:\\Users\\THU-EE-WL\\Downloads\\MSA Dynamic Features'
    """
    generate_static_database_attrs('Changgung')
    generate_static_database_networks('Changgung')
    generate_dynamic_database_attrs(
        rootconfig.path.dynamic_feature_root, 'MSA')
    generate_dynamic_database_networks(
        rootconfig.path.dynamic_feature_root, 'MSA')
    """

    for num in range(4):
        print('Round %d' % (num + 1))
        # test_load_static_attrs()
        # test_load_static_networks()
        test_load_dynamic_attrs()
        # test_load_dynamic_networks()
