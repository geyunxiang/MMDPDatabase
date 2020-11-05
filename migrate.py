import pymongo
import mongodb_database as mdb
import MongoDB as MDB

client = pymongo.MongoClient("localhost", 27017)
db = client['Changgung']
col1 = db['features']
col2 = db['dynamic_attr']
col3 = db['dynamic_net']

db1 = client['Changgung_SA']
db2 = client['Changgung_SN']
db3 = client['Changgung_DA']
db4 = client['Changgung_DN']

""" move static net record """
query1 = dict(feature='BOLD.net')
try:
    while col1.find_one(query1) != None:
        record = col1.find_one_and_delete(query1)
        colname = record['atlas'] + record['feature']
        doc = dict(scan=record['scan'],
                   value=record['value'], comment=record['comment'])
        db2[colname].insert_one(doc)
except OSError:
    pass

""" move static attr record """
query2 = dict()
try:
    while col1.find_one(query2) != None:
        record = col2.find_one_and_delete(query2)
        colname = record['atlas'] + record['feature']
        doc = dict(scan=record['scan'],
                   value=record['value'], comment=record['comment'])
        db1[colname].insert_one(doc)
except OSError:
    pass

""" move dynamic net record """
query3 = dict()
try:
    while col3.find_one(query3) != None:
        record = col3.find_one_and_delete(query3)
        (wl, ss) = (record['window_length,step_size'])
        colname = record['atlas'] + \
            record['feature'] + '(' + wl + ',' + ss + ')'
        doc = dict(scan=record['scan'], value=record['value'],
                   slice=record['slice'], comment=record['comment'])
        db4[colname].insert_one(doc)
except OSError:
    pass

try:
    while col3.find_one(query3) != None:
        record = col3.find_one_and_delete(query3)
        (wl, ss) = (record['window_length,step_size'])
        colname = record['atlas'] + \
            record['feature'] + '(' + wl + ',' + ss + ')'
        doc = dict(scan=record['scan'], value=record['value'],
                   slice=record['slice'], comment=record['comment'])
        db3[colname].insert_one(doc)
except OSError:
    pass
