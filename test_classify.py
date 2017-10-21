import os
import re
import time
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
		prob = [float(i[1]) for i in prob]
		probs.append(prob)
	probs_dict = []
	for prob in probs:
		probs_dict.append(dict(zip(['0','1'],prob)))
	return probs_dict
	
	
def worker(prob,_id):
	db.test.find_one_and_update({'_id': _id,'class':None}, { '$set':{'class':prob}})

if __name__ == '__main__':  #bulk_write
	start_time = datetime.strptime('2017-10-01', "%Y-%m-%d")
	end_time = datetime.strptime('2017-10-04', "%Y-%m-%d")
	query = db.test.find({'tweet.date':{'$gte':start_time,'$lt':end_time},'class':None},{'_id':1,'tweet.text':1})
	ids = []
	texts = []
	for i in query:
		ids.append(i['_id'])
		texts.append(i['tweet']['text'])
	probs = batch_ftpredict(texts)
	requests = [UpdateOne({'_id': _id,'class':None}, {'$set': {'class':probs[index]}}) for index,_id in tqdm(enumerate(ids))]
	result = db.test.bulk_write(requests)
	pprint(result.bulk_api_result)
	client.close()
	
# if __name__ == '__main__':
	# start_time = datetime.strptime('2017-10-01', "%Y-%m-%d")
	# end_time = datetime.strptime('2017-10-04', "%Y-%m-%d")
	# query = db.test.find({'tweet.date':{'$gt':start_time,'$lt':end_time},'class':None},{'_id':1,'tweet.text':1})
	# ids = []
	# texts = []
	# for i in query:
		# ids.append(i['_id'])
		# texts.append(i['tweet']['text'])
	# probs = batch_ftpredict(texts)
	# #pool = Pool(processes=multiprocessing.cpu_count())
	# pool = Pool(processes=48)
	# #[pool.apply(worker,(probs[index],_id)) for index,_id in tqdm(enumerate(ids))]
	# [pool.apply_async(worker,(probs[index],_id)) for index,_id in tqdm(enumerate(ids))]
	# pool.close()
	# pool.join()