import os
import re
import time
import json
from datetime import datetime
from tqdm import tqdm
import codecs 
import numpy as np
import subprocess
import multiprocessing
#from multiprocessing import Pool
from multiprocessing.dummy import Pool

from pprint import pprint
import pymongo
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne

from Config import get_spider_config
_,db,r = get_spider_config()


re_prob = re.compile('(?:__label__(\d)\s([^_]+)[\s]*)')
def batch_ftpredict(texts):
	if type(texts) != list:
		texts = [texts]
	pid = os.getpid()
	tmf  = '/home/ubuntu/work/'+str(pid)+'.txt'
	with codecs.open(tmf,'w','utf-8') as f:
		for line in texts:
			f.write(line+'\n')
		f.close()
	p=subprocess.Popen(['/home/ubuntu/work/fastText-0.1.0/fasttext',
						'predict-prob',
						'/home/ubuntu/work/korea.bin',
						tmf,
						'2'], 
						shell=False, 
						stdout=subprocess.PIPE,
						stderr=subprocess.PIPE)
	result,error = p.communicate()
	#print result,error
	probs = []
	for line in result.splitlines():
		prob = re_prob.findall(line)
		prob = sorted(prob,key=lambda item:item[0])
		prob = [float(i[1]) for i in prob]
		probs.append(prob)
	probs_dict = []
	for prob in probs:
		probs_dict.append(dict(zip(['0','1'],prob)))
	return probs_dict

def classify():
	query = db.korea.find({'class':None},{'_id':1,'tweet.raw_text':1})
	ids = []
	texts = []
	for i in query:
		ids.append(i['_id'])
		texts.append(i['tweet']['raw_text'])
	if len(ids) == 0:
		return None
	probs = batch_ftpredict(texts)
	requests = [UpdateOne({'_id': _id,'class':None}, {'$set': {'class':probs[index]}}) for index,_id in tqdm(enumerate(ids))]
	result = db.korea.bulk_write(requests)
	pprint(result.bulk_api_result)

if __name__ == '__main__':
	print 'classify_worker start!'
	while True:
		queue = r.lpop('task:classify')
		if queue:
			print 'classify_worker process!'
			classify()
			message = json.loads(queue)
			print message
			if message['is_last']:
				r.rpush('task:clustering',json.dumps(message))
		print 'classify_worker wait!'
		time.sleep(1)
	