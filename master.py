import re
import json
from datetime import datetime,timedelta

import pymongo
client = pymongo.MongoClient('52.91.51.100:27017')
db = client.tweet

import redis
r = redis.StrictRedis(host='52.91.102.254', port=6379, db=0)

def get_location(Location):
    Location = Location.strip()
    Location = re.sub('[^a-zA-Z ,]','',Location)
    Locations = Location.split(',')
    Locations = [i.strip() for i in Locations]
    Locations.append(Location)
    return list(set(Locations))
    
def get_types(Type):
    Type = Type.strip()
    Type = re.sub('[^a-zA-Z ,]','',Type)
    Types = Type.split(',')
    Types = [i.strip() for i in Types]
    Types.append(Type)
    return list(set(Types))


def get_query_str(event):
    loc =  get_location(event['event']['Location'])
    loc = set(re.sub(',','',' '.join(loc)).split(' '))
    trigger = get_types(event['event']['Type'])
    trigger = set(re.sub(',','',' '.join(trigger)).split(' '))
    date = event['event']['Date']
    temp = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
    date_since = (temp - timedelta(days=1)).strftime('%Y-%m-%d')
    date_until = (temp + timedelta(days=2)).strftime('%Y-%m-%d')
    return '('+' OR '.join(loc)+')' + ' '+'('+' OR '.join(trigger)+')' + ' '+'since:'+date_since+' '+ 'until:'+date_until


def get_task():
    events = db.event_list.find({'event.Date':{'$gt':'2017-09-25 00:00:00'}},{'_id':1,'event.Location':1,'event.Type':1,'event.Date':1})
    for event in events:
        q = get_query_str(event)
        message = {'q':q,'f':['&f=news','&f=tweets',''],'num':100,'event_id':event['_id']}
        print message
        r.rpush('task:dataset',json.dumps(message))


