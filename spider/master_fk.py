import re
import json
from datetime import datetime,timedelta

from Config import get_fk_config
_,db,r = get_fk_config()


def get_location():
	locs = db.event_metadata.find_one({"name":{"$eq":"location_group_by_char_5"}})
	return list(set([" OR ".join(t) for t in locs['data']]))
	
def get_types():
	types = db.event_metadata.find_one({"name":{"$eq":"type_group_by_char_5"}})
	return list(set([" OR ".join(t) for t in types['data']]))
def get_users():
	users = db.event_metadata.find_one({'name':{"$eq":"freq_users"}})
	return list(set(users['data']))
		

def get_query_str(loc,type):
	return '('+loc+')' +' '+'('+type+')'.encode('utf8')

def get_task():
	now = datetime.now()
	WAIT_TIME_MINUTES = 15
	locs = get_location()
	types = get_types()
	users = get_users()
	while True:
		for loc in locs:
			for type in types:
				q = get_query_str(loc,type)
				message = {'q':q,'f':['&f=news','','&f=tweets'],'num':-1,
				"sinceTimeStamp":(now - timedelta(minutes=WAIT_TIME_MINUTES)).strftime("%Y-%m-%d %H:%M:%S"),
				"untilTimeStamp":now.strftime("%Y-%m-%d %H:%M:%S")
				}
				print(message)
				r.rpush("task:fk",json.dumps(message))				
		for user in users:
			message = {'q':'from:'+user,'f':'&f=tweets','num':-1,
				"sinceTimeStamp":(now - timedelta(minutes=WAIT_TIME_MINUTES)).strftime("%Y-%m-%d %H:%M:%S"),
				"untilTimeStamp":now.strftime("%Y-%m-%d %H:%M:%S")
			}
			print(message)
			r.rpush("task:fk",json.dumps(message))
		time_gone = (datetime.now()-now).seconds
		if time_gone < 60*WAIT_TIME_MINUTES:
			time.sleep(60*WAIT_TIME_MINUTES-time_gone)
			now = datetime.now()
		else:
			now = now+timedelta(minutes=WAIT_TIME_MINUTES)					 
if __name__ == '__main__':
	get_task()

