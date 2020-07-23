'''
MongoDB is a non-relational database used to store feature values.
It stores data in JSON format, with a hierarchy of
db server -> database -> collection -> record.

The record looks like this:
static document
{
    "data_source": "Changgung",
    "scan": "baihanxiang_20190211",
    "atlas": "brodmann_lrce",
    "feature": "BOLD.inter.BC",
    "dynamic": 0,
    "value": "...actual csv str...",
    "comment": {"...descriptive str..."}
}

dynamic document
{
    "data_source":"Changgung",
    "scan": "CMSA_01",
    "atlas": "brodmann_lrce",
    "feature": "BOLD.inter.BC",
    "dynamic": 1, 
    "window_length": 22,
    "step_size": 1,
    "slice_num": the num of the slice 0,1,2,3…
    "value": "...actual csv str...",
    "comment": {"...descriptive str..."}
}
'''
import pymongo
import pickle
import sys
import os
import time
import logging
import scipy.io as scio
from pymongo import monitoring
from mmdps.proc import atlas, netattr


class MongoDBDatabase:
    """
    docstring for MongoDBDatabase
    parameter data_source: 	the only non-default parameter of constructor function
    parameter scan :		mriscan
    parameter atlas_name : 	name of atlas
    parameter feature : 	name of feature file
    parameter dynamic : 	0/1
    parameter window_length : window_length
    parameter step_size : 	step_size
    parameter slice_num : 	the number of slice in a sequence
    parameter comment: 		default {}
    """

    def __init__(self, data_source, host='localhost', user=None, pwd=None, dbname=None, port=27017):
        """ Connect to mongo server """
        if user == None and pwd == None:
            self.client = pymongo.MongoClient(host, port)
        else:
            uri = "mongodb://" + user + ":" + pwd + \
                "@" + host + ":" + str(port)
            if dbname != None:
                uri = uri+"/" + dbname
            self.client = pymongo.MongoClient(uri)
        print(self.client)
        self.data_source = data_source
        self.db = self.client[data_source]
        self.col = self.db['features']
        self.temp_db = self.client['Temp-database']
        self.temp_collection = self.temp_db['Temp-collection']

    def dbStats(self):
        """ Display self.db database status """
        stats = self.db.command("dbstats")
        print(stats)

    def colStats(self, col=None):
        """Display collection status"""
        """Default collection is self.db['features']"""
        if col == None:
            col = 'features'
        stats = self.db.command("collstats", col)
        print(stats)

    def getCol(self, mode):
        "Choose which collection to use"
        "And you can extend with this"
        if mode == 'static':
            self.col = self.db['features']
        elif mode == 'dynamic':
            self.col = self.db['dynamic_data']
        else:
            print("please input correct mode")
        return self.col

    def get_query(self, mode, scan, atlas_name, feature, comment={}, window_length=None, step_size=None):
        """ generate dynamic or static query """
        query = {}
        if mode == 'static':
            query = dict(data_source=self.data_source, scan=scan,
                         atlas=atlas_name, feature=feature, dynamic=0, comment=comment)
        elif mode == 'dynamic':
            query = dict(data_source=self.data_source, scan=scan, atlas=atlas_name, feature=feature,
                         dynamic=1, window_length=window_length, step_size=step_size, comment=comment)
        return query

    def get_document(self, mode, scan, atlas_name, feature, value, comment={}, window_length=None, step_size=None, slice_num=None):
        document = {}
        if mode == 'static':
            document = dict(data_source=self.data_source, scan=scan, atlas=atlas_name,
                            feature=feature, dynamic=0, value=value, comment=comment)
        elif mode == 'dynamic':
            document = dict(data_source=self.data_source, scan=scan, atlas=atlas_name, feature=feature, dynamic=1,
                            window_length=window_length, step_size=step_size, slice_num=slice_num, value=value, comment=comment)
        return document

    def loadmat(self, path):
        dic = scio.loadmat(path)
        dic.pop('__header__')
        dic.pop('__version__')
        dic.pop('__globals__')
        for k in dic.keys():
            dic[k] = pickle.dumps(dic[k])
        return dic

    def quick_query(self, mode, scan):
        """Query only with scan """
        return self.getCol(mode).find(dict(scan=scan))

    def main_query(self, mode, scan, atlas_name, feature):
        """ Query with 3 main keys """
        return self.getCol(mode).find(dict(scan=scan, atlas=atlas_name, feature=feature))

    def total_query(self, mode, scan, atlas_name, feature, comment={}, window_length=None, step_size=None):
        query = self.get_query(mode, scan, atlas_name,
                               feature, comment, window_length, step_size)

        if mode == 'dynamic':
            return self.getCol(mode).find(query).sort("slice_num", 1)
        elif mode == 'static':
             return self.getCol(mode).find(query)

    def exist_query(self, mode, scan, atlas_name, feature, comment={}, window_length=None, step_size=None):
        """ Check a record if exist """
        query = self.get_query(mode, scan, atlas_name,
                               feature, comment, window_length, step_size)
        return self.getCol(mode).find_one(query)

    def insert_document(self, document, col='features'):
        """ 
        Insert a ducument into database collection directly
        This document must be a JSON doc
        """
        self.db[col].insert_one(document)

    def save_static_feature(self, feature, comment={}):
        """
        Feature could be netattr.Net or netattr.Attr
        """
        if self.exist_query('static', feature.scan, feature.atlasobj.name, feature.feature_name, comment) != None:
            raise MultipleRecordException(feature.scan, 'Please check again.')
        attrdata = pickle.dumps(feature.data)
        document = self.get_document(
            'static', feature.scan, feature.atlasobj.name, feature.feature_name, attrdata, comment)
        self.db['features'].insert_one(document)

    def remove_static_feature(self, scan, atlas_name, feature, comment={}):
        query = self.total_query('static', scan, atlas_name, feature, comment)
        self.db['features'].find_one_and_delete(query)

    def save_dynamic_attr(self, attr, comment={}):
        """
        Attr is a netattr.DynamicAttr instance
        """
        if self.exist_query('dynamic', attr.scan, attr.atlasobj.name, attr.feature_name, comment, attr.window_length, attr.step_size) != None:
            raise MultipleRecordException(attr.scan, 'Please check again.')
        for i in range(attr.data.shape[1]):
            # i is the num of the column in data matrix
            value = pickle.dumps(attr.data[:, i])
            slice_num = i
            document = self.get_document(
                'dynamic', attr.scan, attr.atlasobj.name, attr.feature_name, value, comment, attr.window_length, attr.step_size, slice_num)
            self.db['dynamic_data'].insert_one(document)

    def remove_dynamic_attr(self, scan, atlas_name, feature, window_length, step_size, comment={}):
        """
        Fiter and delete all the slice in dynamic_attr
        Default atlas is brodmann_lrce
        """
        query = self.total_query(
            'dynamic', scan, atlas_name, feature, comment, window_length, step_size)
        self.db['dynamic_data'].delete_many(query)

    def save_dynamic_network(self, net, comment={}):
        """
        Net is a netattr.DynamicNet instance
        """
        if self.exist_query('dynamic', net.scan, net.atlasobj.name, net.feature_name, comment, net.window_length, net.step_size) != None:
            raise MultipleRecordException(net.scan, 'Please check again.')
        for i in range(net.data.shape[2]):
            # i is the slice_num of the net
            value = pickle.dumps(net.data[:, :, i])
            slice_num = i
            document = self.get_document(
                'dynamic', net.scan, net.atlasobj.name, net.feature_name, value, comment, net.window_length, net.step_size, slice_num)
            self.db['dynamic_data'].insert_one(document)

    def remove_dynamic_network(self, scan, atlas_name, feature, window_length, step_size, comment={}):
        """
        Fiter and delete all the slice in dynamic_network
        Default atlas is bromann_lrce 
        Default feature is BOLD.net
        """
        query = self.total_query(
            'dynamic', scan, atlas_name, feature, comment, window_length, step_size)
        self.db['dynamic_data'].delete_many(query)

    def save_mat_dict(self, scan, feature, datadict):
        temp_dict = dict(scan=scan, feature=feature)
        if self.db['EEG'].find_one(temp_dict) != None:
            raise MultipleRecordException(temp_dict, 'Please check again.')
        doc = {}
        doc = temp_dict.copy()
        doc.update(datadict)
        self.db['EEG'].insert_one(doc)

    def remove_mat_dict(self, scan, feature):
        query = dict(scan=scan, feature=feature)
        self.db['EEG'].delete_many(query)

    def get_static_attr(self, scan, atlas_name, feature):
        # Return to an attr object  directly
        if self.exist_query('static', scan, atlas_name, feature):
            binary_data = self.main_query(
                'static', scan, atlas_name, feature)['value']
            attrdata = pickle.loads(binary_data)
            atlasobj = atlas.get(atlas_name)
            attr = netattr.Attr(attrdata, atlasobj, scan, feature)
            return attr
        else:
            print("can't find the document you look for. scan: %s, atlas: %s, feature: %s." % (
                scan, atlas_name, feature))
            raise NoRecordFoundException(scan)
            return None

    def get_static_net(self, scan, atlas_name, feature):
        # return to an net object directly
        if self.exist_query('static', scan, atlas_name, feature):
            binary_data = self.main_query(
                'static', scan, atlas_name, feature)['value']
            netdata = pickle.loads(binary_data)
            atlasobj = atlas.get(atlas_name)
            net = netattr.Net(netdata, atlasobj, scan, feature)
            return net
        else:
            print("can't find the document you look for. scan: %s, atlas: %s, feature: %s." % (
                scan, atlas_name, feature))
            raise NoRecordFoundException(scan)
            return None

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

    def get_temp_data(self, description_dict):
        """
        Get temporary data with description_dict
        Return a dict with value:temp_data (de-serialized)
        """
        result = self.temp_collection.find_one(description_dict)
        result['value'] = pickle.loads(result['value'])
        return result

    def remove_temp_data(self, description_dict={}):
        """
        Delete all temp records according to description_dict
        If None is input, delete all temp data
        """
        self.temp_collection.delete_many(description_dict)

    def drop_collection(self, db, col):
        """ Drop a collection in a database """
        self.client[db].drop_collection(col)

    def drop_database(self, dbname):
        """ Drop a database in this mongo client """
        self.client.drop_database(dbname)

    def server_info(self):
        """ Get information about mongodb server we connected to """
        self.client.server_info()


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


if __name__ == '__main__':
    pass
