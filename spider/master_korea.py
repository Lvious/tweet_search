import re
import json
import fire
import time
from tqdm import tqdm
from datetime import datetime,timedelta

from Config import get_spider_config
_,db,r = get_spider_config()


freq_users = set([i['tweet']['user']['screen_name'] for i in db.dataset_korea_m_1.find({},{"tweet.user.screen_name":1})])
            
def get_query_str(loc,triggers,target):
	# start = (now - time_delta).strftime("%Y-%m-%d %H:%M:%S")
	# now_str = now.strftime("%Y-%m-%d %H:%M:%S")
	return '('+loc+')' + + ' '+'('+' OR '.join(triggers)+')'+' '+'('+target+')'

def get_task():
    locs=["North Korea"]
    triggers=["test","launch","fire"]
    targets = ["messile","satellite","rocket","nuclear"]
    now = datetime.now()
    WAIT_TIME = 15
    while True:
        for loc in locs:
            for target in targets:
                q = get_query_str(loc,triggers,target)
                message = {'q':q,'f':['&f=news','','&f=tweets'],'num':-1,
                "sinceTimeStamp":(now - timedelta(minutes=WAIT_TIME)).strftime("%Y-%m-%d %H:%M:%S"),
                "untilTimeStamp":now.strftime("%Y-%m-%d %H:%M:%S")
                }
                r.rpush("task:korea",json.dumps(message))
        for user in freq_users:
            message = {'q':'from:'+user,'f':'&f=tweets','num':-1,
                "sinceTimeStamp":(now - timedelta(minutes=WAIT_TIME)).strftime("%Y-%m-%d %H:%M:%S"),
                "untilTimeStamp":now.strftime("%Y-%m-%d %H:%M:%S")
            }
            r.rpush('task:korea',json.dumps(message))
        time_gone = (datetime.now()-now).seconds
        if time_gone < 60*WAIT_TIME:
            time.sleep(60*WAIT_TIME-time_gone)
            now = datetime.now()
        else:
            now = now+timedelta(minutes=WAIT_TIME)
if __name__ == '__main__':
	get_task()
