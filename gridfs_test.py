from pymongo import MongoClient
import gridfs
db=MongoClient().gridfs_example
fs=gridfs.GridFS(db)
#a=fs.put(b"hello world")
#print(fs.get(a).read())
#a=fs.put(r"D:\test\bold_net.csv")
"""
with open(r"D:\test\bold_net.csv",'rb') as f:
    data=f.read()
    a=fs.put(data,filename='D:\test\bold_net.csv')
    print(a)
"""
"""
with open(r"D:\test\test.mp4",'rb') as f:
    data=f.read()
    a=fs.put(data,filename='test.mp4')
    print(a)
"""

"""
db=MongoClient().test
col=db.students
col.update_one({"name":'wangli'},{'$set':{"name":"wangli6666"}})
"""
"""
db=MongoClient().test
col=db.students
a=col.find_one({'name':'wangli6666'})
#对于这个已有的文档，可以直接修改键值对
a['age']=20
col.replace_one({'name':'wangli6666'},a)
"""




