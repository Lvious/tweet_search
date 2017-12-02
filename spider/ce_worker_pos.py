import time
import json
from bson import json_util
    
import multiprocessing
from multiprocessing import Pool

from Config import get_config
got,db,r = get_config()

def advance_search_dataset(q,f,num,event_id):
    _,db,_ = get_config()
    collection = db.pos
    tweetCriteria = got.manager.TweetCriteria().setQuerySearch(q).setTweetType(f).setMaxTweets(num)
    tweets = got.manager.TweetManager.getTweets(tweetCriteria)
    for tweet in tweets:
        if collection.find_one({'_id':tweet['id']}) == None:
            collection.insert_one({'_id':tweet['id'],'tweet':tweet,'event_id':json.loads(event_id,object_hook=json_util.object_hook),'f':f,'q':q})
    db.close()

def run_pos_task(message_data):
    try:
        q = message_data['q']
        num = message_data['num']
        event_id = message_data['event_id']
        if type(message_data['f']) != list:
            advance_search_dataset(q,message_data['f'],num,event_id)
        else:
            pool = Pool(processes=multiprocessing.cpu_count())
            [pool.apply(advance_search_dataset,(q,f,num,event_id)) for f in message_data['f']]
            pool.close()
            pool.join()
        return True
    except Exception,e:
        return e.message
  
if __name__ == '__main__':
    print 'craw_worker start!'
    while True:
        queue = r.lpop('task:pos')
        if queue:
            print 'craw_worker process!'
            craw = run_pos_task(json.loads(queue))
            if type(craw) != str:
                db.pos_log.insert_one({'message':json.loads(queue),'status':1})
            else:
                db.pos_log.insert_one({'message':json.loads(queue),'status':0,'error':craw})
        time.sleep(1)
        print 'craw_worker wait!'
    db.close()