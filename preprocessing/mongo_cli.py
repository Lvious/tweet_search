db.cloneCollection('ip:host','from coll','to coll')
db.copyDatabase('from db','to db','ip:host')
db.copyDatabase(<from_dbname>, <to_dbname>, <from_hostname>, <username>,<password>);

db.copyDatabase(
   "tweet",
   "tweet",
   "example.net",
   "admin",
   "lixiepeng",
   "MONGODB-CR"
)

mongodump -h IP --port 端口 -u 用户名 -p 密码 -d 数据库 -o 文件存在路径
mongorestore -h IP --port 端口 -u 用户名 -p 密码 -d 数据库 --drop 文件存在路径 