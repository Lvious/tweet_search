import sys
if sys.version_info[0] < 3:
	import got
else:
	import got3 as got

import pymongo

import redis


def get_config():
	#mongo
	client = pymongo.MongoClient('10.42.91.238:27017')
	db = client.tweet
	db.authenticate(name='admin',password='lixiepeng')
	
	#redis
	r = redis.StrictRedis(host='10.42.214.43', port=6379, db=0, password='lixiepeng')
	
	return got,db,r