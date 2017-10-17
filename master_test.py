import re
import json
import fire
from datetime import datetime,timedelta

import pymongo
#client = pymongo.MongoClient('101.132.114.125:27017')
client = pymongo.MongoClient('34.224.37.110:27017')
db = client.tweet

import redis
r = redis.StrictRedis(host='52.91.102.254', port=6379, db=0)

location_group_by_char = db.event_metadata.find_one({'name':'location_group_by_char'})['data']
type_group_by_char = db.event_metadata.find_one({'name':'type_group_by_char'})['data']
freq_users = db.event_metadata.find_one({'name':'freq_users'})['data']

for loc in location_group_by_char:
	for i,v in enumerate(loc):
		if ' ' in v:
			loc[i] = '"'+v+'"'
			
for type_ in type_group_by_char:
	for i,v in enumerate(type_):
		if ' ' in v:
			type_[i] = '"'+v+'"'
			


def get_query_str(loc,trigger,day):
	temp = datetime.strptime(day, "%Y-%m-%d")
	date_since = day
	date_until = (temp + timedelta(days=1)).strftime('%Y-%m-%d')
	return '('+' OR '.join(loc)+')' + ' OR '+'('+' OR '.join(trigger)+')' + ' '+'since:'+date_since+' '+ 'until:'+date_until

def get_user_query_str(user,day):
	temp = datetime.strptime(day, "%Y-%m-%d")
	date_since = day
	date_until = (temp + timedelta(days=1)).strftime('%Y-%m-%d')
	return 'from:' + user + ' '+'since:'+date_since+' '+ 'until:'+date_until

def get_task():
	start_time = datetime.strptime('2017-10-01', "%Y-%m-%d")
	days = [(start_time + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(2)]
	for day in days:
		for user in freq_users:
				q = get_user_query_str(user,day)
				message = {'q':q,'f':'&f=tweets','num':20}
				print message
				r.rpush('task:test',json.dumps(message))
		for loc in location_group_by_char:
			for trigger in type_group_by_char:
				q = get_query_str(loc,trigger,day)
				message = {'q':q,'f':['&f=news','','&f=tweets'],'num':20}
				print message
				r.rpush('task:test',json.dumps(message))

if __name__ == '__main__':
	get_task()

