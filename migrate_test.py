import pymongo

client = pymongo.MongoClient("localhost", 27017)
db = client['test']
col1 = db['stu']
col2 = db['stu2']
query = dict(age=19)
try:
    while col1.find_one_and_delete(query) != None:
        record = col1.find_one_and_delete(query)
        col2.insert_one(record)
except OSError:
    pass
col1.rename('stu3')
