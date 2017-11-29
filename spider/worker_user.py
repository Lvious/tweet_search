import time
import json
    
import multiprocessing
from multiprocessing import Pool

from Config import get_config
got,db,r = get_config()


def advance_search_test(type_id,q,f,num):
	_,db,_ = get_config()
	collection = [db.Entertainment,db.Religion,db.Sport,db.Military,db.Politics,db.Education,db.Technology,db.Economy,db.Agriculture][type_id-1]
	tweetCriteria = got.manager.TweetCriteria().setQuerySearch(q).setTweetType(f).setMaxTweets(num)
	tweets = got.manager.TweetManager.getTweets(tweetCriteria)
	for tweet in tweets:
		if collection.find_one({'_id':tweet.id}) == None:
			collection.insert_one({'_id':tweet.id,'tweet':tweet.__dict__,'f':f,'q':q})

def run_user_task(message_data):
	try:
		q = message_data['q']
		num = message_data['num']
		type_id  = message_data['type_id']
		if type(message_data['f']) != list:
			advance_search_test(type_id,q,message_data['f'],num)
		else:
			pool = Pool(processes=multiprocessing.cpu_count())
			[pool.apply(advance_search_test,(q,f,num)) for f in message_data['f']]
			pool.close()
			pool.join()
	except Exception,e:
		raise Exception(e.message)
  
if __name__ == '__main__':
	print 'craw_worker start!'
	while True:
		queue = r.lpop('task:user')
		if queue:
			print 'craw_worker process!'
			try:
				run_user_task(json.loads(queue))
				db.user_log.insert_one({'message':json.loads(queue),'status':1})
			except Exception,e:
				db.user_log.insert_one({'message':json.loads(queue),'status':0,'error':e.message})
		time.sleep(1)
		print 'craw_worker wait!'