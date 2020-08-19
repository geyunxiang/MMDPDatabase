import pymongo

client = pymongo.MongoClient("localhost", 27017)
db = client['Changgung']
col1 = db['dynamic_data']
col2 = db['dynamic_net']
query = dict(feature="BOLD.net")
try:
    while col1.find_one(query) != None:
        record = col1.find_one_and_delete(query)
        col2.insert_one(query)
except OSError:
    pass
col1.rename('dynamic_attr')
