import time
import json
    
import multiprocessing
from multiprocessing import Pool

from Config import get_spider_config
got,db,r = get_spider_config()

def advance_search_dataset(q,f,num,event_id):
    _,db,_ = get_config()
    collection = db.dataset_
    tweetCriteria = got.manager.TweetCriteria().setQuerySearch(q).setTweetType(f).setMaxTweets(num)
    tweets = got.manager.TweetManager.getTweets(tweetCriteria)
    for tweet in tweets:
        if collection.find_one({'_id':tweet.id}) == None:
            collection.insert_one({'_id':tweet.id,'tweet':tweet.__dict__,'event_id':event_id,'f':f,'q':q})

def run_dataset_task(message_data):
    #message_data['f'] = #'' '&f=tweets' '&f=news' '&f=broadcasts' '&f=videos' '&f=images' '&f=users'
    #message_data['q']  = #''
    #message_data['num'] =10
    try:
        q = message_data['q']
        num = message_data['num']
        event_id = message_data['event_id']
        if type(message_data['f']) != list:
            advance_search_dataset(q,message_data['f'],num,event_id)
        else:
            pool = Pool(processes=multiprocessing.cpu_count())
            #[pool.apply_async(advance_search_dataset,(q,f,num,event_id)) for f in message_data['f']]
            [pool.apply(advance_search_dataset,(q,f,num,event_id)) for f in message_data['f']]
            pool.close()
            pool.join()
        return True
    except:
        return False
  
if __name__ == '__main__':
    print 'craw_worker start!'
    while True:
        queue = r.lpop('task:dataset')
        if queue:
            print 'craw_worker process!'
            craw = run_dataset_task(json.loads(queue))
            if craw:
                db.dataset_log.insert_one({'message':json.loads(queue),'status':1})
            else:
                db.dataset_log.insert_one({'message':json.loads(queue),'status':0})
        time.sleep(1)
        print 'craw_worker wait!'