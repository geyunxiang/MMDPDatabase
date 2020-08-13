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
import scipy.io as scio

from mmdps.proc import atlas, netattr


class MongoDBDatabase:
    """
    docstring for MongoDBDatabase
    :param data_source: 	    default parameter of constructor function
    :parame scan :		        mriscan
    :parame atlas_name : 	    name of atlas
    :parame feature : 	        name of feature file
    :parame dynamic :       	0/1
    :parame window_length :     window_length
    :parame step_size : 	    step_size
    :parame slice_num : 	    number of slice in a sequence
    :parame comment: 		    default {}
    """

    def __init__(self, data_source, host='localhost', user=None, pwd=None, dbname=None, port=27017):
        """ Connect to mongo server """
        if user == None and pwd == None:
            self.client = pymongo.MongoClient(host, port)
        else:
            uri = 'mongodb://%s:%s@%s:%s' % (user, pwd, host, str(port))
            if dbname != None:
                uri = uri+"/" + dbname
            self.client = pymongo.MongoClient(uri)
        #with open("EEG_conf.json", 'r') as f:
        #    self.EEG_conf = json.loads(f.read())
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
        elif mode == 'dynamic1':
            self.col = self.db['dynamic_attr']
        elif mode == 'dynamic2':
            self.col = self.db['dynamic_net']
        else:
            print("please input correct mode")
        return self.col

    def get_query(self, mode, scan, atlas_name, feature, comment={}, window_length=None, step_size=None):
        """ generate dynamic or static query """
        query = {}
        if mode == 'static':
            query = dict(data_source=self.data_source, scan=scan,
                         atlas=atlas_name, feature=feature, dynamic=0, comment=comment)
        elif mode == 'dynamic1' or mode == 'dynamic2' or mode == 'dynamic':
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
        """ load mat, return data dict"""
        dic = scio.loadmat(path)
        dic.pop('__header__')
        dic.pop('__version__')
        dic.pop('__globals__')
        return dic

    def find(self, db, col, query):
        """ Gereral query"""
        db = self.client[db]
        col = db[col]
        return col.find(query)

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
            if feature.find('net') == -1:
                return self.getCol('dynamic1').find(query).sort("slice_num", 1)
            else:
                return self.getCol('dynamic2').find(query).sort("slice_num", 1)
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
        query = dict(scan=scan, atlas=atlas_name,
                     feature=feature, comment=comment)
        self.db['features'].find_one_and_delete(query)

    def save_dynamic_attr(self, attr, comment={}):
        """
        Attr is a netattr.DynamicAttr instance
        """
        query = self.get_query('dynamic', attr.scan, attr.atlasobj.name,
                               attr.feature_name, comment, attr.window_length, attr.step_size)
        for idx in range(attr.data.shape[1]):
            query['slice_num'] = idx
            if self.db['dynamic_attr'].find_one(query) != None:
                raise MultipleRecordException(
                    attr.scan, 'Please check again.')
            value = pickle.dumps(attr.data[:, idx])
            slice_num = idx
            document = self.get_document(
                'dynamic', attr.scan, attr.atlasobj.name, attr.feature_name, value, comment, attr.window_length, attr.step_size, slice_num)
            self.db['dynamic_attr'].insert_one(document)

    def remove_dynamic_attr(self, scan, atlas_name, feature, window_length, step_size, comment={}):
        """
        Fiter and delete all the slice in dynamic_attr
        Default atlas is brodmann_lrce
        """
        query = dict(scan=scan, atlas=atlas_name, feature=feature,
                     comment=comment, window_length=window_length, step_size=step_size)
        self.db['dynamic_attr'].delete_many(query)

    def save_dynamic_network(self, net, comment={}):
        """ Net is a netattr.DynamicNet instance """
        query = self.get_query('dynamic', net.scan, net.atlasobj.name,
                               net.feature_name, comment, net.window_length, net.step_size)
        for idx in range(net.data.shape[2]):
            query['slice_num'] = idx
            if self.db['dynamic_net'].find_one(query) != None:
                raise MultipleRecordException(net.scan, 'Please check again.')
            else:
                value = pickle.dumps(net.data[:, :, idx])
                slice_num = idx
                document = self.get_document(
                    'dynamic', net.scan, net.atlasobj.name, net.feature_name, value, comment, net.window_length, net.step_size, slice_num)
                self.db['dynamic_net'].insert_one(document)

    def remove_dynamic_network(self, scan, atlas_name, feature, window_length, step_size, comment={}):
        """
        Fiter and delete all the slice in dynamic_network
        Default atlas is bromann_lrce 
        Default feature is BOLD.net
        """
        query = dict(scan=scan, atlas=atlas_name, feature=feature,
                     comment=comment, window_length=window_length, step_size=step_size)
        self.db['dynamic_net'].delete_many(query)

    def save_mat_dict(self, scan, mat, datadict):
        """ mat : name of mat file"""
        feature = self.EEG_conf[mat]['feature']
        dic = dict(scan=scan, feature=feature)
        if self.db['EEG'].find_one(dic) != None:
            raise MultipleRecordException(dic, 'Please check again.')
        if self.EEG_conf[mat]['fields'] == []:
            for k in datadict.keys():
                dic[feature] = pickle.dumps(datadict[k])
        else:
            for k in datadict.keys():
                DataArray = datadict[k]
                for field in self.EEG_conf[mat]['fields']:
                    dic[field] = pickle.dumps(DataArray[field])
        self.db['EEG'].insert_one(dic)

    def remove_mat_dict(self, scan, feature):
        query = dict(scan=scan, feature=feature)
        self.db['EEG'].delete_many(query)

    def get_mat(self, scan, mat, field):
        """ Get mat from mongo """
        query = dict(scan=scan, feature=mat)
        count = self.db['EEG'].count_documents(query)
        currentMat = mat+'.mat'
        dic = {}
        if count == 0:
            raise NoRecordFoundException((scan, mat))
        elif count > 1:
            raise MultipleRecordException((scan, mat))
        else:
            record = self.db['EEG'].find_one(query)
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

    def get_attr(self, scan, atlas_name, feature):
        """  Return to an attr object  directly """
        query = dict(scan=scan, atlas=atlas_name, feature=feature)
        collection = self.db['features']
        count = collection.count_documents(query)
        if count == 0:
            raise NoRecordFoundException(scan)
        elif count > 1:
            raise MultipleRecordException(scan)
        else:
            AttrData = pickle.loads(collection.find(query)['value'])
            atlasobj = atlas.get(atlas_name)
            attr = netattr.Attr(AttrData, atlasobj, scan, feature)
            return attr

    def get_dynamic_attr(self, scan, atlas_name, feature, window_length, step_size):
        """ Return to dynamic attr object directly """
        query = dict(scan=scan, atlas=atlas_name, feature=feature,
                     window_length=window_length, step_size=step_size)
        collection = self.db['dynamic_attr']
        if collection.find_one(query) == None:
            raise NoRecordFoundException(scan)
        else:
            records = collection.find(
                query).sort([('slice_num', pymongo.ASCENDING)])
            atlasobj = atlas.get(atlas_name)
            attr = netattr.DynamicAttr(
                None, atlasobj, window_length, step_size, scan, feature)
            for record in records:
                attr.append_one_slice(pickle.loads(record['value']))
            return attr

    def get_net(self, scan, atlas_name, feature):
        """  Return to an net object directly  """
        query = dict(scan=scan, atlas=atlas_name, feature=feature)
        count = self.db['features'].count_documents(query)
        if count == 0:
            raise NoRecordFoundException(scan)
        elif count > 1:
            raise MultipleRecordException(scan)
        else:
            NetData = pickle.loads(self.db['features'].find(query)['value'])
            atlasobj = atlas.get(atlas_name)
            net = netattr.Net(NetData, atlasobj, scan, feature)
            return net

    def get_dynamic_net(self, scan, atlas_name, window_length, step_size, feature='BOLD.net'):
        """ Return to dynamic attr object directly """
        query = dict(scan=scan, atlas=atlas_name, feature=feature,
                     window_length=window_length, step_size=step_size)
        collection = self.db['dynamic_net']
        if collection.find_one(query) == None:
            raise NoRecordFoundException((scan, atlas, feature))
        else:
            records = collection.find(query).sort(
                [('slice_num', pymongo.ASCENDING)])
            atlasobj = atlas.get(atlas_name)
            net = netattr.DynamicNet(
                None, atlasobj, window_length, step_size, scan, feature)
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
