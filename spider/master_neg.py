import re
import json
import random
random.seed(1)

import pymongo
client = pymongo.MongoClient('34.224.37.110:27017')
db = client.tweet

stopwords_en = db.event_metadata.find_one({'name':'stopwords_en'})['data']

import redis
r = redis.StrictRedis(host='52.91.102.254', port=6379, db=0)

def get_stopwords():
	return stopwords_en[random.randint(0,len(stopwords_en)-1)]


def get_task():
	for item in db.dataset.find({},{'_id':1}):
		if random.randint(0,1):
			tweet_id = item['_id']
			q = get_stopwords()
			message = {'q':q,'f':'&f=tweets','num':10,'tweet_id':tweet_id}
			print message
			r.rpush('task:neg',json.dumps(message))

if __name__ == '__main__':
	get_task()