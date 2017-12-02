import sys
if sys.version_info[0] < 3:
	import got
else:
	import got3 as got

import pymongo

import redis

import os

def get_config():
	#mongo
	client = pymongo.MongoClient(os.environ['MONGOHOST'],27017)
	db = client.tweet
	db.authenticate(name='admin',password='lixiepeng')
	
	#redis
	r = redis.StrictRedis(host=os.environ['REDISHOST'], port=6379, db=0, password='lixiepeng')
	
	return got,db,r