import re
import json
import fire
import time
from tqdm import tqdm
from datetime import datetime,timedelta

from Config import get_spider_config
_,db,r = get_spider_config()



            
def get_query_str(loc,triggers,target):
	# start = (now - time_delta).strftime("%Y-%m-%d %H:%M:%S")
	# now_str = now.strftime("%Y-%m-%d %H:%M:%S")
	return '('+loc+')' + + ' '+'('+' OR '.join(triggers)+')'+' '+'('+target+')'

def get_task():
    locs=["North Korea"]
    triggers=["test","launch","fire"]
    target = ["messile","satellite","rocket","nuclear"]
    while True:
        now = datetime.now()
        for loc in locs:
            for target in targets:
                q = get_query_str(loc,triggers,target)
                message = {'q':q,'f':['&f=news','','&f=tweets'],'num':1000,
                "sinceTimeStamp":(now - timedelta(minutes=60)).strftime("%Y-%m-%d %H:%M:%S"),
                "untilTimeStamp":now.strftime("%Y-%m-%d %H:%M:%S")
                }
                r.rpush("task:korea",json.dumps(message))
        time.sleep(60*60)
if __name__ == '__main__':
	get_task()
