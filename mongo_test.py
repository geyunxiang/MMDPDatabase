"""
MongoDB test script goes here.
"""
import os
import time
import json
import MongoDB as MDB

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
    Scan rootconfig.path.feature_root and move the directory to MongoDB
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
    Scan rootconfig.path.feature_root and move the directory to MongoDB
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


def test_load_static_attrs(feature_root = rootconfig.path.feature_root, data_source='Changgung'):
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
                    # print('! not found! scan: %s, atlas: %s, attr: %s not found!' % (mriscan, atlas_name, attr_name))
    query_end = time.time()
    query_time = query_end - query_start
    print('Query %d static attrs (netattr.Attr) using loader time cost: %1.2fs' % (load_counter, query_time))

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
    print('Query %d static attrs (netattr.Attr) using MongoDB time cost: %1.2fs' % (load_counter, query_time))    


def test_load_static_networks(feature_root = rootconfig.path.feature_root, data_source='Changgung'):
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
    print('Query %d static networks (netattr.Net) using loader time cost: %1.2fs' % (load_counter, query_time))

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
    print('Query %d static networks (netattr.Net) using MongoDB time cost: %1.2fs' % (load_counter, query_time))  


def test_load_dynamic_attrs(dynamic_rootfolder = rootconfig.path.dynamic_feature_root, data_source='MSA'):
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
    print('Query %d dynamic attrs (netattr.DynamicAttr) using loader time cost: %1.2fs' % (load_counter, query_end - query_start))

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
    print('Query %d dynamic attrs (netattr.DynamicAttr) using MongoDB time cost: %1.2fs' % (load_counter, query_end - query_start))


def test_load_dynamic_networks(dynamic_rootfolder = rootconfig.path.dynamic_feature_root, data_source='MSA'):
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
    print('Query %d dynamic networks (netattr.DynamicNet) using loader time cost: %1.2fs' % (load_counter, query_end - query_start))

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
    print('Query %d dynamic networks (netattr.DynamicNet) using MongoDB time cost: %1.2fs' % (load_counter, query_end - query_start))


if __name__ == '__main__':
    dynamic_rootfolder = "C:\\Users\\THU-EE-WL\\Downloads\\MSA Dynamic Features"
    rootfolder = "C:\\Users\\THU-EE-WL\\Desktop\\EEG"
    for num in range(4):
        print('Round %d' % (num + 1))
        # test_load_static_attrs()
        # test_load_static_networks()
        # test_load_dynamic_attrs()
        test_load_dynamic_networks()
    """
    generate_EEG_database(rootfolder)
    generate_dynamic_database_attrs(dynamic_rootfolder)
    generate_dynamic_database_networks(dynamic_rootfolder)
    mdb = MDB.MongoDBDatabase('Changgung')
    mdb.get_mat('EEG_feature_examples', 'Freq', 'Freq')
    test_load_dynamic_networks(dynamic_rootfolder, 'Changgung')
    test_load_dynamic_attrs(dynamic_rootfolder)
    """
    """
    'test_load_dynamic_attrs(dynamic_rootfolder)
    
    test_load_dynamic_networks(dynamic_rootfolder, 'MSA')
    """
