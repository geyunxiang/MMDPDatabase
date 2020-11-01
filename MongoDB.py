import pymongo
import pickle
import os
import sys
import time
import json
import scipy.io as scio
import numpy as np

from mmdps.proc import atlas, netattr


dbname = ['static_attr', 'static_net', 'dynamic_attr',
          'dynamic_net', 'EEG', 'Temp-database']


class MongoDBDatabase:

    def __init__(self, data_source, host='127.0.0.1', user=None, pwd=None, dbname=None, port=27017):
        """ Connect to mongo server and display connecting server """
        if user == None and pwd == None:
            self.client = pymongo.MongoClient(host, port)
        else:
            uri = 'mongodb://%s:%s@%s:%s' % (user, pwd, host, str(port))
            if dbname != None:
                uri += '/' + dbname
            self.client = pymongo.MongoClient(uri)
        print(self.client)
        with open("EEG_conf.json", 'r') as f:
            self.EEG_conf = json.loads(f.read())
        self.data_source = data_source
        self.sadb = self.client[self.data_source + '_SA']
        self.sndb = self.client[self.data_source + '_SN']
        self.dadb = self.client[self.data_source + '_DA']
        self.dndb = self.client[self.data_source + '_DN']
        self.EEG_db = self.client[self.data_source + '_EEG']
        self.temp_db = self.client[self.data_source + '_TEMP']
        self.temp_collection = self.temp_db['Temp-collection']

    """ delete dbstats()"""
    """ delete colstats()"""

    def query(self, dbname, colname, filter_query):
        db = self.client[dbname]
        col = db[colname]
        return col.find(filter_query)

    """ delete main_query() """
    """ delete quick_query() """

    def exist_query(self, dbname, scan, atlas_name, feature, comment={}, window_length=None, step_size=None):
        """ dbname could be SA SN DA DN EEG TMEP"""
        """ return only one of query records """
        """ return None if no matching doucment is found """
        query = dict(scan=scan, comment=comment)
        db = self.getdb(dbname)
        col = self.getcol(atlas_name, feature, window_length, step_size)
        return db[col].find_one(query)

    def total_query(self, dbname, scan, atlas_name, feature, comment={}, window_length=None, step_size=None):
        """ dbname could be SA SN DA DN TMEP """
        """ return all of query records """
        query = dict(scan=scan, comment=comment)
        db = self.getdb(dbname)
        col = self.getcol(atlas_name, feature, window_length, step_size)
        return db[col].find(query)

    def getcol(self, atlas_name, attrname, window_length=None, step_size=None):
        colname = atlas_name + '-' + attrname
        if (window_length, step_size) != (None, None):
            colname += '-'+str((window_length, step_size))
        return colname

    def getdb(self, dbname):
        """ dbname could be SA SN DA DN EEG TEMP"""
        db = self.data_source + '_' + dbname
        return self.client[db]

    def save_static_attr(self, attr, comment={}):
        atlas_name = attr.atlasobj.name
        attrname = attr.feature_name
        col = self.getcol(atlas_name, attrname)
        if self.exist_query('SA', attr.scan, atlas_name, attrname, comment) != None:
            raise MultipleRecordException(attr.scan, 'Please check again.')
        attrdata = pickle.dumps(attr.data)
        doc = dict(scan=attr.scan, value=attrdata, comment=comment)
        self.sadb[col].insert_one(doc)

    def remove_static_attr(self, scan, atlas_name, feature, comment={}):
        col = self.getcol(atlas_name, feature)
        query = dict(scan=scan, comment=comment)
        self.sadb[col].find_one_and_delete(query)

    def save_static_net(self, net, comment={}):
        atlas_name = net.atlasobj.name
        attrname = net.feature_name
        col = self.getcol(atlas_name, attrname)
        if self.exist_query('SN', net.scan, atlas_name, attrname, comment) != None:
            raise MultipleRecordException(net.scan, 'Please check again.')
        netdata = pickle.dumps(net.data)
        doc = dict(scan=net.scan, value=netdata, comment=comment)
        self.sndb[col].insert_one(doc)

    def remove_static_net(self, scan, atlas_name, feature, comment={}):
        col = self.getcol(atlas_name, feature)
        query = dict(scan=scan, comment=comment)
        self.sndb[col].find_one_and_delete(query)

    def save_dynamic_attr(self, attr, comment={}):
        """ Attr could be Dynamic Attr instance """
        atlas_name = attr.atlasobj.name
        attrname = attr.feature_name
        (wl, ss) = (attr.window_length, attr.step_size)
        col = self.getcol(atlas_name, attrname, wl, ss)
        if self.exist_query('DA', attr.scan, atlas_name, attrname, wl, ss) != None:
            raise MultipleRecordException(
                attr.scan, 'Please check again.')
        docs = []
        for idx in range(attr.data.shape[1]):
            value = pickle.dumps(attr.data[:, idx])
            doc = dict(scan=attr.scan, value=value, slice=idx, comment=comment)
            docs.append(doc)
        self.dadb[col].insert_many(docs)

    def remove_dynamic_attr(self, scan, atlas_name, feature, window_length, step_size, comment={}):
        col = self.getcol(atlas_name, feature, window_length, step_size)
        query = dict(scan=scan, comment=comment)
        self.dadb[col].delete_many(query)

    def save_dynamic_net(self, net, comment={}):
        atlas_name = net.atlasobj.name
        attrname = net.feature_name
        (wl, ss) = (net.window_length, net.step_size)
        col = self.getcol(atlas_name, attrname, wl, ss)
        if self.exist_query('DN', net.scan, atlas_name, attrname, wl, ss) != None:
            raise MultipleRecordException(net.scan, 'Please check again.')
        docs = []
        for idx in range(net.data.shape[2]):
            value = pickle.dumps(net.data[:, :, idx])
            doc = dict(scan=net.scan, value=value, slice=idx, comment=comment)
            docs.append(doc)
        self.dndb[col].insert_many(docs)

    def remove_dynamic_net(self, scan, atlas_name, feature, window_length, step_size, comment={}):
        col = self.getcol(atlas_name, feature, window_length, step_size)
        query = dict(scan=scan, comment=comment)
        self.dndb[col].delete_many(query)

    def loadmat(self, path):
        """ load mat, return data dict"""
        dic = scio.loadmat(path)
        dic.pop('__header__')
        dic.pop('__version__')
        dic.pop('__globals__')
        return dic

    def save_mat_dict(self, scan, mat, datadict):
        """ mat: .mat file name """
        """ db:  self.EEG_db  col:feature """
        feature = self.EEG_conf[mat]['feature']
        dic = dict(scan=scan)
        if self.EEG_db[feature].find_one(dic) != None:
            raise MultipleRecordException(dic, 'Please check again.')
        if self.EEG_conf[mat]['fields'] == []:
            for k in datadict.keys():
                dic[feature] = pickle.dumps(datadict[k])
        else:
            for k in datadict.keys():
                DataArray = datadict[k]
                for field in self.EEG_conf[mat]['fields']:
                    dic[field] = pickle.dumps(DataArray[field])
        self.EEG_db[feature].insert_one(dic)

    def remove_mat_dict(self, scan, feature):
        """remove mat record"""
        query = dict(scan=scan)
        self.EEG_db[feature].delete_many(query)

    def get_mat(self, scan, mat, field):
        """ Get mat and its field from mongo """
        """ it doesn't work well """
        query = dict(scan=scan)
        feature = mat
        count = self.EEG_db[feature].count_documents(query)
        currentMat = mat+'.mat'
        dic = {}
        if count == 0:
            raise NoRecordFoundException((scan, mat))
        elif count > 1:
            raise MultipleRecordException((scan, mat))
        else:
            record = self.EEG_db[feature].find_one(query)
            if field in record.keys():
                matname = '%s_%s.mat' % (mat, field)
                if self.EEG_conf[currentMat]['fields'] != []:
                    dic[field] = pickle.loads(record[field])[0, 0]
                else:
                    dic[field] = pickle.loads(record[field])
                scio.savemat(matname, dic)
                return dic
            else:
                print('%s not in %s' % (field, mat))
                return None

    def get_static_attr(self, scan, atlas_name, feature, comment={}):
        """  Return to an attr object  directly """
        query = dict(scan=scan, comment=comment)
        col = self.getcol(atlas_name, feature)
        count = self.sadb[col].count_documents(query)
        if count == 0:
            raise NoRecordFoundException(scan+atlas_name+feature)
        elif count > 1:
            raise MultipleRecordException(scan+atlas_name+feature)
        else:
            AttrData = pickle.loads(self.sadb[col].find_one(query)['value'])
            atlasobj = atlas.get(atlas_name)
            attr = netattr.Attr(AttrData, atlasobj, scan, feature)
            return attr

    def get_dynamic_attr(self, scan, atlas_name, feature, window_length, step_size, comment={}):
        """ Return to dynamic attr object directly """
        query = dict(scan=scan, comment=comment)
        col = self.getcol(atlas_name, feature, window_length, step_size)
        count = self.dadb[col].find(query)
        if count == 0:
            raise NoRecordFoundException(scan + atlas_name + feature)
        else:
            records = self.dadb[col].find(
                query).sort([('slice', pymongo.ASCENDING)])
            atlasobj = atlas.get(atlas_name)
            attr = netattr.DynamicAttr(
                None, atlasobj, window_length, step_size, scan, feature)
            for record in records:
                attr.append_one_slice(pickle.loads(record['value']))
            return attr

    def get_static_net(self, scan, atlas_name, comment={}):
        """  Return to an static net object directly  """
        query = dict(scan=scan, comment=comment)
        col = self.getcol(atlas_name, 'BOLD.net')
        count = self.sndb[col].count_documents(query)
        if count == 0:
            raise NoRecordFoundException(scan+atlas_name+'BOLD.net')
        elif count > 1:
            raise MultipleRecordException(scan+atlas_name+'BOLD.net')
        else:
            NetData = pickle.loads(self.sndb[col].find_one(query)['value'])
            atlasobj = atlas.get(atlas_name)
            net = netattr.Net(NetData, atlasobj, scan, 'BOLD.net')
            return net

    def get_dynamic_net(self, scan, atlas_name, window_length, step_size, comment={}):
        """ Return to dynamic attr object directly """
        query = dict(scan=scan, comment=comment)
        col = self.getcol(atlas_name, 'BOLD.net', window_length, step_size)
        if self.dndb[col].find_one(query) == None:
            raise NoRecordFoundException((scan, atlas, 'BOLD.net'))
        else:
            records = self.dndb[col].find(query).sort(
                [('slice', pymongo.ASCENDING)])
            atlasobj = atlas.get(atlas_name)
            net = netattr.DynamicNet(
                None, atlasobj, window_length, step_size, scan, 'BOLD.net')
            for record in records:
                net.append_one_slice(pickle.loads(record['value']))
            return net

    def put_temp_data(self, temp_data, description_dict, overwrite=False):
        """
        Insert temporary data into MongoDB. 
        Input temp_data as a serializable object (like np.array).
        The description_dict should be a dict whose keys do not contain 'value', which is used to store serialized data
        """
        # check if record already exists, given description_dict
        count = self.temp_collection.count_documents(description_dict)
        if count > 0 and not overwrite:
            raise MultipleRecordException(
                description_dict, 'Please consider a new name')
        elif count > 0 and overwrite:
            self.temp_collection.delete_many(description_dict)
        description_dict.update(dict(value=pickle.dumps(temp_data)))
        self.temp_collection.insert_one(description_dict)

    def remove_temp_data(self, description_dict={}):
        """
        Delete all temp records according to description_dict
        If None is input, delete all temp data
        """
        self.temp_collection.delete_many(description_dict)

    def drop_collection(self, dbname, col):
        """ Drop a collection in a database """
        """ dbname could be SA SN DA DN TMEP EEG """
        """ database should have administrator authorization"""
        self.getdb[dbname].drop_collection(col)

    def drop_database(self, dbname):
        """ Drop a database with mongo client """
        """ dbname could be SA SN DA DN TMEP EEG """
        """ database should have administrator authorization"""
        self.client.drop_database(self.getdb(dbname))

    def createIndex(self, dbname, col, index):
        """ Create index on collection field """
        db = self.getdb(dbname)
        db[col].create_index([(idx, pymongo.ASCENDING) for idx in index])
        """
        Usage: db[col].create_index([('field1', pymongo.ASCENDING), ('field2', pymongo.ASCENDING), ...])
        """


class MultipleRecordException(Exception):
    """
    """

    def __init__(self, name, suggestion=''):
        super(MultipleRecordException, self).__init__()
        self.name = name
        self.suggestion = suggestion

    def __str__(self):
        return 'Multiple record found for %s. %s' % (self.name, self.suggestion)

    def __repr__(self):
        return 'Multiple record found for %s. %s' % (self.name, self.suggestion)


class NoRecordFoundException(Exception):
    """
    """

    def __init__(self, name, suggestion=''):
        super(NoRecordFoundException, self).__init__()
        self.name = name
        self.suggestion = ''

    def __str__(self):
        return 'No record found for %s. %s' % (self.name, self.suggestion)

    def __repr__(self):
        return 'No record found for %s. %s' % (self.name, self.suggestion)
