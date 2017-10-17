import sys
if sys.version_info[0] < 3:
    import got
else:
    import got3 as got
    
import time
import json
    
import multiprocessing
from multiprocessing import Pool

import pymongo
#client = pymongo.MongoClient('101.132.114.125:27017')
client = pymongo.MongoClient('52.91.51.100:27017')
db = client.tweet

import redis
r = redis.StrictRedis(host='52.91.102.254', port=6379, db=0)

def advance_search_test(q,f,num):
    #client = pymongo.MongoClient('101.132.114.125:27017')
    client = pymongo.MongoClient('52.91.51.100:27017')
    db = client.tweet
    collection = db.test
    tweetCriteria = got.manager.TweetCriteria().setQuerySearch(q).setTweetType(f).setMaxTweets(num)
    tweets = got.manager.TweetManager.getTweets(tweetCriteria)
    for tweet in tweets:
        if collection.find_one({'_id':tweet.id}) == None:
            collection.insert_one({'_id':tweet.id,'tweet':tweet.__dict__,'f':f,'q':q})

def run_test_task(message_data):
    try:
        q = message_data['q']
        num = message_data['num']
        if type(message_data['f']) != list:
            advance_search_test(q,message_data['f'],num)
        else:
            pool = Pool(processes=multiprocessing.cpu_count())
            [pool.apply(advance_search_test,(q,f,num)) for f in message_data['f']]
            pool.close()
            pool.join()
        return True
    except:
        return False
  
if __name__ == '__main__':
    print 'craw_worker start!'
    while True:
        queue = r.lpop('task:test')
        if queue:
            print 'craw_worker process!'
            craw = run_test_task(json.loads(queue))
            if craw:
                db.test_log.insert_one({'message':json.loads(queue),'status':1})
            else:
                db.test_log.insert_one({'message':json.loads(queue),'status':0})
        time.sleep(1)
        print 'craw_worker wait!'