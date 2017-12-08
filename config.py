import os
import sys

import pymongo

import redis

def get_spider_config():
	#got
	if sys.version_info[0] < 3:
		import got
	else:
		import got3 as got
		
	#mongo
	client = pymongo.MongoClient(os.environ['MONGOHOST'],27017)
	db = client.tweet
	db.authenticate(name='admin',password='lixiepeng')
	
	#redis
	r = redis.StrictRedis(host=os.environ['REDISHOST'], port=6379, db=0, password='lixiepeng')
	
	return got,db,r

def get_config():
	#mongo
	client = pymongo.MongoClient('localhost',27017)
	db = client.tweet
	
	#redis
	r = redis.StrictRedis(host='localhost', port=6379, db=0)
	
	return db,r