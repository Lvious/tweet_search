import re
import json
import random
random.seed(1)

from Config import get_config
_,db,r = get_config()

stopwords_en = db.event_metadata.find_one({'name':'stopwords_en'})['data']

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