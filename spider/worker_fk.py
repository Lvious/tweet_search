import time
import json
    
import multiprocessing
from multiprocessing import Pool

from Config import get_spider_config
_,db,r = get_fk_config()

def advance_search_dataset(q,f,num,s,u):
    collection = db.fk
    tweetCriteria = got.manager.TweetCriteria().setQuerySearch(q).setTweetType(f).setSinceTimeStamp(s).setUntilTimeStamp(u).setMaxTweets(num)
    tweets = got.manager.TweetManager.getTweets(tweetCriteria)
    for tweet in tweets:
		if collection.find_one({'_id':tweet['id']}) == None:
            collection.insert_one({'_id':tweet['id'],'tweet':tweet,'f':f,'q':q})

def run_dataset_task(message_data):
    #message_data['f'] = #'' '&f=tweets' '&f=news' '&f=broadcasts' '&f=videos' '&f=images' '&f=users'
    #message_data['q']  = #''
    #message_data['num'] =10
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
	print 'fk_craw_worker start!'
	while True:
		queue = r.lpop('task:fk')
		if queue:
			print 'craw_worker process!'
			try:
				run_korea_task(json.loads(queue))
				db.fk_log.insert_one({'message':json.loads(queue),'status':1})
			except Exception,e:
				db.fk_log.insert_one({'message':json.loads(queue),'status':0,'error':e.message})
			if r.llen('task:fk'):
				message = {"is_last":True}
				r.rpush('task:fk:classify',json.dumps(message))
			else:
				message = {"is_last":False}
				r.rpush('task:fk:classify',json.dumps(message))

		time.sleep(1)
		print 'fk_craw_worker wait!'
