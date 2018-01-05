import sys
if sys.version_info[0] < 3:
	import got
else:
	import got3 as got
    
import time
import json
    
import multiprocessing
from multiprocessing import Pool
from datetime import datetime

from Config import get_spider_config
_,db,r = get_spider_config()

def advance_search_korea(q,f,num,s,u):

	collection = db.korea
	tweetCriteria = got.manager.TweetCriteria().setQuerySearch(q).setTweetType(f).setSinceTimeStamp(s).setUntilTimeStamp(u).setMaxTweets(num)
	tweets = got.manager.TweetManager.getTweets(tweetCriteria)
	for tweet in tweets:
		if collection.find_one({'_id':tweet['id']}) == None:
			collection.insert_one({'_id':tweet['id'],'tweet':tweet,'f':f,'q':q})

def run_korea_task(message_data):
	try:
		q = message_data['q']
		num = message_data['num']
		sinceTimeStamp = datetime.strptime(message_data['sinceTimeStamp'],"%Y-%m-%d %H:%M:%S")
		untilTimeStamp = datetime.strptime(message_data['untilTimeStamp'],"%Y-%m-%d %H:%M:%S")
		if type(message_data['f']) != list:
			advance_search_korea(q,message_data['f'],num,sinceTimeStamp,untilTimeStamp)
		else:
			pool = Pool(processes=multiprocessing.cpu_count())
			[pool.apply(advance_search_korea,(q,f,num,sinceTimeStamp,untilTimeStamp)) for f in message_data['f']]
			pool.close()
			pool.join()
	except Exception,e:
		raise Exception(e.message)
  
if __name__ == '__main__':
	print 'craw_worker start!'
	while True:
		queue = r.lpop('task:korea')
		if queue:
			print 'craw_worker process!'
			try:
				run_korea_task(json.loads(queue))
				db.korea_log.insert_one({'message':json.loads(queue),'status':1})
			except Exception,e:
				db.korea_log.insert_one({'message':json.loads(queue),'status':0,'error':e.message})
			if r.llen('task:korea') == 0:
				message = {"is_last":True}
				r.rpush('task:classify',json.dumps(message))
			else:
				message = {"is_last":False}
				r.rpush('task:classify',json.dumps(message))	

		time.sleep(1)
		print 'craw_worker wait!'
