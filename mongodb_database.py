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
        """
        Display collection status
        Default collection is self.col and default db is self.db
        """
        if col == None:
            col = self.col
        else:
            col = self.db[col]
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

    def get_query(self, mode, scan, atlas_name, feature, window_length=None, step_size=None):
        """ generate dynamic or static query """
        query = {}
        if mode == 'static':
            query = dict(data_source=self.data_source, scan=scan,
                         atlas=atlas_name, feature=feature, dynamic=0)
        elif mode == 'dynamic':
            query = dict(data_source=self.data_source, scan=scan, atlas=atlas_name, feature=feature,
                         dynamic=1, window_length=window_length, step_size=step_size,)
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

    def quick_query(self, mode, scan):
        """Query only with scan """
        return self.getCol(mode).find(dict(scan=scan))

    def main_query(self, mode, scan, atlas_name, feature):
        """ Query with 3 main keys """
        return self.getCol(mode).find(dict(scan=scan, atlas=atlas_name, feature=feature))

    def total_query(self, mode, scan, atlas_name, feature, window_length=None, step_size=None):
        query = self.get_query(mode, scan, atlas_name,
                               feature, window_length, step_size)
        document = self.getCol(mode).find(query)
        if mode == 'dynamic':
            self.getCol(mode).find(query).sort("slice_num", 1)
        return document

    def exist_query(self, mode, scan, atlas_name, feature, window_length=None, step_size=None):
        """ Check a record if exist """
        query = self.get_query(mode, scan, atlas_name,
                               feature, window_length, step_size)
        return self.getCol(mode).find_one(query)

    def save_static_feature(self, feature, comment={}):
        """
        feature could be netattr.Net or netattr.Attr
        """
        mode = 'static'
        if self.exist_query(mode, feature.scan, feature.atlasobj.name, feature.feature_name) != None:
            raise MultipleRecordException(feature.scan, 'Please check again.')
        attrdata = pickle.dumps(feature.data)
        document = self.get_document(
            mode, feature.scan, feature.atlasobj.name, feature.feature.name, attrdata, comment)
        self.getCol(mode).insert_one(document)

    def remove_static_feature(self, scan, atlas_name, feature):
        mode = 'static'
        self.getCol(mode).find_one_and_delete(
            self.main_query(mode, scan, atlas_name, feature))

    def save_dynamic_attr(self, attr, comment={}):
        """
        Attr is a netattr.DynamicAttr instance
        """
        mode = 'dynamic'
        if self.exist_query(mode, attr.scan, attr.atlasobj.name, attr.feature_name, attr.window_length, attr.step_size):
            raise MultipleRecordException(attr.scan, 'Please check again.')
        for i in range(attr.data.shape[1]):
            # i is the num of the column in data matrix
            value = pickle.dumps(attr.data[:, i])
            slice_num = i
            document = self.get_document(
                mode, attr.scan, attr.atlasobj.name, attr.feature_name, value, comment, attr.window_length, attr.step_size, slice_num)
            self.getCol(mode).insert_one(document)

    def remove_dynamic_attr(self, scan, feature, window_length, step_size, atlas_name='brodmann_lrce'):
        """
        Fiter and delete all the slice in dynamic_attr
        Default atlas is brodmann_lrce
        """
        mode = 'dynamic'
        self.getCol(mode).delete_many(self.get_query(
            mode, scan, atlas_name, feature, window_length, step_size))

    def save_dynamic_network(self, net, comment={}):
        """
        Net is a netattr.DynamicNet instance
        """
        mode = 'dynamic'
        if self.exist_query(mode, net.scan, net.atlasobj.name, net.feature_name, net.window_length, net.step_size):
            raise MultipleRecordException(net.scan, 'Please check again.')
        for i in range(net.data.shape[2]):
            # i is the slice_num of the net
            value = pickle.dumps(net.data[:, :, i])
            slice_num = i
            document = self.get_document(
                mode, net.scan, net.atlasobj.name, net.feature_name, value, comment, net.window_length, net.step_size, slice_num)
            self.getCol(mode).insert_one(document)

    def remove_dynamic_network(self, scan, window_length, step_size, atlas_name='brodmann_lrce', feature='BOLD.net'):
        """
        Fiter and delete all the slice in dynamic_network
        Default atlas is bromann_lrce 
        Default feature is BOLD.net
        """
        mode = 'dynamic'
        self.getCol(mode).delete_many(self.get_query(
            mode, scan, atlas_name, feature, window_length, step_size))

    def get_static_attr(self, scan, atlas_name, feature):
        # Return to an attr object  directly
        mode = 'static'
        if self.exist_query(mode, scan, atlas_name, feature):
            binary_data = self.main_query(
                mode, scan, atlas_name, feature)['value']
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
        mode = 'static'
        if self.exist_query(mode, scan, atlas_name, feature):
            binary_data = self.main_query(
                mode, scan, atlas_name, feature)['value']
            netdata = pickle.loads(binary_data)
            atlasobj = atlas.get(atlas_name)
            net = netattr.Net(netdata, atlasobj, scan, feature)
            return net
        else:
            print("can't find the document you look for. scan: %s, atlas: %s, feature: %s." % (
                scan, atlas_name, feature))
            raise NoRecordFoundException(scan)
            return None

    def put_temp_data(self, temp_data, name, description=None):
        """
        Insert temporary data into MongoDB. 
        Input temp_data as a serializable object (like np.array) and name as a string.
        The description argument is optional
        """
        # check if name is already in temp database
        if self.temp_collection.count_documents(dict(name=name)) > 0:
            raise MultipleRecordException(name, 'Please consider a new name')
        document = dict(value=pickle.dumps(temp_data),
                        name=name, description=description)
        self.temp_collection.insert_one(document)

    def get_temp_data(self, name):
        """
        Get temporary data with name
        Return a dict with keys = value:np.array, name:str, description:str
        """
        result = self.temp_collection.find_one(dict(name=name))
        result['value'] = pickle.loads(result['value'])
        return result

    def remove_temp_data(self, name):
        """
        Delete all temp records with the input name
        If None is input, delete all temp data
        """
        if name is None:
            self.temp_collection.delete_many({})
        else:
            self.temp_collection.delete_many(dict(name=name))


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
