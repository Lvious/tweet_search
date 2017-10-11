import sys
if sys.version_info[0] < 3:
    import got
else:
    import got3 as got
    
import multiprocessing
from multiprocessing import Pool

import pymongo
client = pymongo.MongoClient('52.91.51.100:27017')
db = client.tweet

import redis
rd = redis.StrictRedis(host='52.91.102.254', port=6379, db=0)

def advance_search_dataset(q,f,num,event_id):
    collection = db.dataset
    if len(f) > 0:
        tweet_type = f.split('=')[1]
    else:
        tweet_type = 'popular'
    tweetCriteria = got.manager.TweetCriteria().setQuerySearch(q).setTweetType(f).setMaxTweets(num)
    tweets = got.manager.TweetManager.getTweets(tweetCriteria)
    for tweet in tweets:
        if collection.find_one({'_id':tweet.id}) == None:
            collection.insert_one({'_id':tweet.id,'tweet':tweet.__dict__,'event_id':event_id,'tweet_type':tweet_type})

def run_dataset_task(message_data):
    #message_data['f'] = #'' '&f=tweets' '&f=news' '&f=broadcasts' '&f=videos' '&f=images' '&f=users'
    #message_data['q']  = #''
    #message_data['num'] =10
    q = message_data['q']
    num = message_data['num']
    event_id = message_data['event_id']
    pool = Pool(processes=multiprocessing.cpu_count())
    [pool.apply_async(advance_search_dataset,(q,f,num,event_id)) for f in message_data['f']]
    pool.close()
    pool.join()
    return True
    
def advance_search_stream(q,f,num=-1):
    collection = db.stream
    if len(f) > 0:
        tweet_type = f.split('=')[1]
    else:
        tweet_type = 'popular'
    tweetCriteria = got.manager.TweetCriteria().setQuerySearch(q).setTweetType(f).setMaxTweets(num)
    tweets = got.manager.TweetManager.getTweets(tweetCriteria)
    for tweet in tweets:
        if collection.find_one({'_id':tweet.id}) == None:
            collection.insert_one({'_id':tweet.id,'tweet':tweet.__dict__,'tweet_type':tweet_type,'q':q})

def run_stream_task(message_data):
    #message_data['f'] = #'' '&f=tweets' '&f=news' '&f=broadcasts' '&f=videos' '&f=images' '&f=users'
    #message_data['q']  = #''
    #message_data['num'] =10
    q = message_data['q']
    num = message_data['num']
    pool = Pool(processes=multiprocessing.cpu_count())
    [pool.apply_async(advance_search_dataset,(q,f,num)) for f in message_data['f']]
    pool.close()
    pool.join()
    return True
  
if __name__ == '__main__':
    print 'craw_worker start!'
    while True:
        message = rd.get_message()
        if message:
            print 'craw_worker process!'
            if message['channel'] == 'dataset' and message['type'] == 'message':
                craw = run_dataset_task(message['data'])
                if craw:
                    rd.publish('dataset_', 'craw '+message['data']+' success')
            elif message['channel'] == 'stream' and message['type'] == 'message':
                craw = run_stream_task(message['data'])
                if craw:
                    rd.publish('stream_', 'craw '+message['data']+' success')
            else:
                pass
        print 'craw_worker wait!'
        time.sleep(1)