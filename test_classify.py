import os
import re
import time
from datetime import datetime
from tqdm import tqdm
import fire
import codecs 
import numpy as np
import subprocess
import multiprocessing
from multiprocessing import Pool

import pymongo
client = pymongo.MongoClient('34.224.37.110:27017')
db = client.tweet

re_prob = re.compile('(?:__label__(\d)\s([^_]+)[\s]*)')
def ftpredict(texts): #if texts == list :  macro probs
	if type(texts) != list:
		texts = [texts]
	pid = os.getpid()
	tmf  = '/home/ubuntu/lxp/work/'+str(pid)+'.txt'
	with codecs.open(tmf,'w','utf-8') as f:
		for line in texts:
			f.write(line+'\n')
		f.close()
	p=subprocess.Popen(['/home/ubuntu/lxp/fastText/fasttext',
						'predict-prob',
						'/home/ubuntu/lxp/work/terrorist.simple.bin',
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
		prob = [i[1] for i in prob]
		prob = np.array(prob,dtype='float32')
		probs.append(prob)
	#print probs
	prob_s = np.array(probs)
	prob_s = sum(prob_s)/np.sum(probs)
	prob_s = [float(i) for i in prob_s]
	return dict(zip(['0','1'],prob_s))

def batch_ftpredict(texts):
	if type(texts) != list:
		texts = [texts]
	pid = os.getpid()
	tmf  = '/root/lxp/'+str(pid)+'.txt'
	with codecs.open(tmf,'w','utf-8') as f:
		for line in texts:
			f.write(line+'\n')
		f.close()
	p=subprocess.Popen(['/root/lxp/fastText/fasttext',
						'predict-prob',
						'/root/lxp/terrorist.simple.bin',
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
		prob = [i[1] for i in prob]
		probs.append(prob)
	results = []
	for prob in probs:
		results.append(dict(zip(['0','1'],prob)))
	return results
	
	
def worker(query):
	ids = []
	texts = []
	for i in query:
		ids.append(i['_id'])
		texts.append(i['tweet']['text'])
	probs = batch_ftpredict(texts)
	for index,_id in tqdm(enumerate(ids)):
		db.test.find_one_and_update({'_id': _id,'class':None}, { '$set':{'class':probs[index]}})
def master():
	start_time = datetime.strptime('2017-10-01', "%Y-%m-%d")
	end_time = datetime.strptime('2017-10-04', "%Y-%m-%d")
	query = db.test.find({'tweet.date':{'$gt':start_time,'$lt':end_time},'class':None},{'_id':1,'tweet.text':1}).limit(100)
	while query.count() != 0:
		worker(query)
		query = db.test.find({'tweet.date':{'$gt':start_time,'$lt':end_time},'class':None},{'_id':1,'tweet.text':1}).limit(100)       
def main(processes=8):
	pool = Pool(processes=processes)
	[pool.async_apply(master,) for i in range(processes)]
	pool.close()
	pool.join()
if __name__ == '__main__':
    fire.Fire(main)