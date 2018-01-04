import re
import json
import fire
from tqdm import tqdm
from datetime import datetime,timedelta

from Config import get_config
_,db,r = get_spider_config()

location_group_by_char_5 = db.event_metadata.find_one({'name':'location_group_by_char_5'})['data']
type_group_by_char_5 = db.event_metadata.find_one({'name':'type_group_by_char_5'})['data']
freq_users = [i['tweet']['user']['standard_text'] for i in db.dataset_korea_m_1.find({},{"tweet.user.screen_name":1})]

for loc in location_group_by_char_5:
	for i,v in enumerate(loc):
		if ' ' in v:
			loc[i] = '"'+v+'"'
			
for type_ in type_group_by_char_5:
	for i,v in enumerate(type_):
		if ' ' in v:
			type_[i] = '"'+v+'"'
			

            
def get_query_str(loc,trigger,now,time_delta):
	start = (now - time_delta).strftime("%Y-%m-%d %H:%M:%S")
	return '('+' OR '.join(loc)+')' + ' '+'('+' OR '.join(trigger)+')' + ' '+'since:'+start+' '+ 'until:'+now


def get_task():
    locs=["North Korea"]
    triggers=["test","launch","fired","messile"]
    while True:
        now = datetime.datetime.now()
        for loc in locs:
            for trigger in triggers:
                q = get_query_str(locs,triggers,now,timedelta(minutes=15))
                message = {'q':q,'f':['&f=news','','&f=tweets'],'num':1000}
                r.push("task:korea",json.dumps(message))
        time.sleep(15*60)
if __name__ == '__main__':
	get_task()