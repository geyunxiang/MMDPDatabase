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
        """ Connect to mongo server """
        if user == None and pwd == None:
            self.client = pymongo.MongoClient(host, port)
        else:
            uri = 'mongodb://%s:%s@%s:%s' % (user, pwd, host, str(port))
            if dbname != None:
                uri = uri+"/" + dbname
            self.client = pymongo.MongoClient(uri)
        print(self.client)
        with open("EEG_conf.json", 'r') as f:
            self.EEG_conf = json.loads(f.read())
        self.data_source = data_source
        self.temp_db = self.client[self.data_source + '_TEMP']
        self.EEG_db = self.client[self.data_source + '_EEG']
        self.sadb = self.client[self.data_source + '_SA']
        self.sndb = self.client[self.data_source + '_SN']
        self.dadb = self.client[self.data_source + '_DA']
        self.dndb = self.client[self.data_source + '_DN']
        self.temp_collection = self.temp_db['Temp-collection']

    def query(self, dbname, colname, filter):
        db = self.client[dbname]
        col = db[colname]
        return col.find(filter)

    def exist_query(self, dbname, scan, atlas_name, feature, comment={}, window_length=None, step_size=None):
        """ dbname could be SA SN DA DN EEG TMEP"""
        query = dict(scan=scan, comment=comment)
        db = self.getdb(dbname)
        col = self.getcol(atlas_name, feature, (window_length, step_size))
        return db[col].find_one(query)

    def getcol(self, atlas_name, attrname, window_length=None, step_size=None):
        colname = atlas_name + attrname
        if (window_length, step_size) != (None, None):
            colname += '('+str(window_length)+','+str(step_size)+')'
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

    def createIndex(self, dbname, col, index):
        db = self.getdb(dbname)
        db[col].create_index(index, pymongo.ASCENDING)


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
