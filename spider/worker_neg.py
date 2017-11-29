import time
import json
    
import multiprocessing
from multiprocessing import Pool

from Config import get_config
got,db,r = get_config()

refreshCursor_base = 'TWEET-%s-919086151339782146-BD1UO2FFu9QAAAAAAAAVfAAAAAcAAABWAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=='

def advance_search_dataset(q,f,num,tweet_id):
    _,db,_ = get_config()
    collection = db.neg
    tweetCriteria = got.manager.TweetCriteria().setQuerySearch(q).setTweetType(f).setMaxTweets(num)
    refreshCursor = refreshCursor_base%(tweet_id)
    tweets = got.manager.TweetManager.getTweets(tweetCriteria,refreshCursor=refreshCursor)
    for tweet in tweets:
        if collection.find_one({'_id':tweet.id}) == None:
            collection.insert_one({'_id':tweet.id,'tweet':tweet.__dict__,'f':f,'q':q})

def run_neg_task(message_data):
    #message_data['f'] = #'' '&f=tweets' '&f=news' '&f=broadcasts' '&f=videos' '&f=images' '&f=users'
    #message_data['q']  = #''
    #message_data['num'] =10
    try:
        q = message_data['q']
        num = message_data['num']
        tweet_id = message_data['tweet_id']
        #event_id = message_data['event_id']
        if type(message_data['f']) != list:
            advance_search_dataset(q,message_data['f'],num,tweet_id)
        else:
            pool = Pool(processes=multiprocessing.cpu_count())
            #[pool.apply_async(advance_search_dataset,(q,f,num,event_id)) for f in message_data['f']]
            [pool.apply(advance_search_dataset,(q,f,num,tweet_id)) for f in message_data['f']]
            pool.close()
            pool.join()
        return True
    except:
        return False
  
if __name__ == '__main__':
    print 'craw_worker start!'
    while True:
        queue = r.lpop('task:neg')
        if queue:
            print 'craw_worker process!'
            craw = run_neg_task(json.loads(queue))
            if craw:
                db.neg_log.insert_one({'message':json.loads(queue),'status':1})
            else:
                db.neg_log.insert_one({'message':json.loads(queue),'status':0})
        time.sleep(1)
        print 'craw_worker wait!'