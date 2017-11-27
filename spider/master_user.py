import re
import json
import fire
from tqdm import tqdm
from datetime import datetime,timedelta

import pymongo
client = pymongo.MongoClient('101.132.187.45:27017')
db = client.twitter

import redis
r = redis.StrictRedis(host='52.91.102.254', port=6379, db=0)


def get_task():
	for type_id in tqdm(range(10)[1:]):
		for user in tqdm(db.train_user.find_one({'_id':type_id})['list']):
			message = {'type_id':type_id,'q':'from:'+user,'f':'&f=tweets','num':-1}
			r.rpush('task:user',json.dumps(message))

if __name__ == '__main__':
	get_task()
