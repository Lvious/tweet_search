import re
import json
from datetime import datetime,timedelta

from tqdm import tqdm

from Config import get_config
_,db,r = get_config()


def get_task():
	for item in tqdm(db.current_event.find({'type':{'$gte':1}},{'_id':1,'query_str':1})):
		q = item['query_str']
		message = {'q':q,'f':['&f=news','','&f=tweets'],'num':1000,'event_id':item['_id']}
		print message
		r.rpush('task:pos',json.dumps(message))

if __name__ == '__main__':
	get_task()

