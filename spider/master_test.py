import re
import json
import fire
from tqdm import tqdm
from datetime import datetime,timedelta

from Config import get_config
_,db,r = get_config()

location_group_by_char_5 = db.event_metadata.find_one({'name':'location_group_by_char_5'})['data']
type_group_by_char_5 = db.event_metadata.find_one({'name':'type_group_by_char_5'})['data']
freq_users = db.event_metadata.find_one({'name':'freq_users'})['data']

for loc in location_group_by_char_5:
	for i,v in enumerate(loc):
		if ' ' in v:
			loc[i] = '"'+v+'"'
			
for type_ in type_group_by_char_5:
	for i,v in enumerate(type_):
		if ' ' in v:
			type_[i] = '"'+v+'"'
			


def get_query_str(loc,trigger,day):
	temp = datetime.strptime(day, "%Y-%m-%d")
	date_since = day
	date_until = (temp + timedelta(days=1)).strftime('%Y-%m-%d')
	return '('+' OR '.join(loc)+')' + ' '+'('+' OR '.join(trigger)+')' + ' '+'since:'+date_since+' '+ 'until:'+date_until

def get_user_query_str(user,day):
	temp = datetime.strptime(day, "%Y-%m-%d")
	date_since = day
	date_until = (temp + timedelta(days=1)).strftime('%Y-%m-%d')
	return 'from:' + user + ' '+'since:'+date_since+' '+ 'until:'+date_until

def get_task():
	start_time = datetime.strptime('2017-10-01', "%Y-%m-%d")
	days = [(start_time + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(16)]
	for day in tqdm(days):
		for user in tqdm(freq_users):
				q = get_user_query_str(user,day)
				message = {'q':q,'f':'&f=tweets','num':40}
				#print message
				r.rpush('task:test',json.dumps(message))
		for loc in tqdm(location_group_by_char_5):
			for trigger in tqdm(type_group_by_char_5):
				q = get_query_str(loc,trigger,day)
				message = {'q':q,'f':['&f=news','','&f=tweets'],'num':40}
				#print message
				r.rpush('task:test',json.dumps(message))

if __name__ == '__main__':
	get_task()

