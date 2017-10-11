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

import redis
r = redis.StrictRedis(host='52.91.102.254', port=6379, db=0)
p = r.pubsub()
p.subscribe('dataset', 'stream')

def advance_search_dataset(q,f,num,event_id):
    client = pymongo.MongoClient('52.91.51.100:27017')
    db = client.tweet
    collection = db.dataset
    tweetCriteria = got.manager.TweetCriteria().setQuerySearch(q).setTweetType(f).setMaxTweets(num)
    tweets = got.manager.TweetManager.getTweets(tweetCriteria)
    for tweet in tweets:
        if collection.find_one({'_id':tweet.id}) == None:
            collection.insert_one({'_id':tweet.id,'tweet':tweet.__dict__,'event_id':event_id,'f':f,'q':q})

def run_dataset_task(message_data):
    #message_data['f'] = #'' '&f=tweets' '&f=news' '&f=broadcasts' '&f=videos' '&f=images' '&f=users'
    #message_data['q']  = #''
    #message_data['num'] =10
    q = message_data['q']
    num = message_data['num']
    event_id = message_data['event_id']
    if type(message_data['f']) != list:
        advance_search_dataset(q,message_data['f'],num,event_id)
    else:
        pool = Pool(processes=multiprocessing.cpu_count())
        [pool.apply_async(advance_search_dataset,(q,f,num,event_id)) for f in message_data['f']]
        pool.close()
        pool.join()
    return True
    
def advance_search_stream(q,f,num=-1):
    client = pymongo.MongoClient('52.91.51.100:27017')
    db = client.tweet
    collection = db.stream
    tweetCriteria = got.manager.TweetCriteria().setQuerySearch(q).setTweetType(f).setMaxTweets(num)
    tweets = got.manager.TweetManager.getTweets(tweetCriteria)
    for tweet in tweets:
        if collection.find_one({'_id':tweet.id}) == None:
            collection.insert_one({'_id':tweet.id,'tweet':tweet.__dict__,'f':f,'q':q})

def run_stream_task(message_data):
    #message_data['f'] = #'' '&f=tweets' '&f=news' '&f=broadcasts' '&f=videos' '&f=images' '&f=users'
    #message_data['q']  = #''
    #message_data['num'] =10
    q = message_data['q']
    num = message_data['num']
    if type(message_data['f']) != str:
        advance_search_dataset(q,message_data['f'],num,event_id)
    else:
        pool = Pool(processes=multiprocessing.cpu_count())
        [pool.apply_async(advance_search_dataset,(q,f,num,event_id)) for f in message_data['f']]
        pool.close()
        pool.join()
    return True
  
if __name__ == '__main__':
    print 'craw_worker start!'
    while True:
        message = p.get_message()
        if message:
            print 'craw_worker process!'
            if message['channel'] == 'dataset' and message['type'] == 'message':
                craw = run_dataset_task(json.loads(message['data']))
                if craw:
                    r.publish('dataset_', 'craw '+message['data']+' success')
            elif message['channel'] == 'stream' and message['type'] == 'message':
                craw = run_stream_task(json.loads(message['data']))
                if craw:
                    r.publish('stream_', 'craw '+message['data']+' success')
            else:
                pass
        print 'craw_worker wait!'
        time.sleep(1)