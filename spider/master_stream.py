import re
import json
import fire
import time
from tqdm import tqdm
from datetime import datetime,timedelta
from collections import Counter
from Config import get_spider_config
_,db,r = get_spider_config()


            
def get_query_str():
	return 'a OR b OR c OR d OR e OR f OR g OR h OR i OR j OR k OR l OR m OR n OR o OR p OR q OR r OR s OR t OR u OR v OR w OR x OR y OR z'

def get_task():

    WAIT_TIME = 15
    while True:
        q = get_query_str(loc,triggers,target)
                message = {'q':q,'f':['&f=news','','&f=tweets'],'num':-1,
                "sinceTimeStamp":(now - timedelta(minutes=WAIT_TIME)).strftime("%Y-%m-%d %H:%M:%S"),
                "untilTimeStamp":now.strftime("%Y-%m-%d %H:%M:%S")
                }
		print(message)
                r.rpush("task:korea",json.dumps(message))
        for user in freq_users:
            message = {'q':'from:'+user,'f':'&f=tweets','num':-1,
                "sinceTimeStamp":(now - timedelta(minutes=WAIT_TIME)).strftime("%Y-%m-%d %H:%M:%S"),
                "untilTimeStamp":now.strftime("%Y-%m-%d %H:%M:%S")
            }
	    print(message)
            r.rpush('task:korea',json.dumps(message))
        time_gone = (datetime.now()-now).seconds
        if time_gone < 60*WAIT_TIME:
            time.sleep(60*WAIT_TIME-time_gone)
            now = datetime.now()
        else:
            now = now+timedelta(minutes=WAIT_TIME)
if __name__ == '__main__':
	get_task()
