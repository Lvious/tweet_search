import os
import re
import time
from datetime import datetime
from tqdm import tqdm
import codecs 
import numpy as np
import subprocess
import multiprocessing
from multiprocessing.dummy import Pool

import pymongo
client = pymongo.MongoClient('34.224.37.110:27017')
db = client.tweet

re_prob = re.compile('(?:__label__(\d)\s([^_]+)[\s]*)')
def ftpredict(texts):
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
    
def worker(i):
	print i['_id']
	probs = ftpredict(i['tweet']['text'])
	print probs
	db.test.find_one_and_update({'_id': i['_id'],'probs':None}, { '$set':{'probs':probs)}})
	print 'done!'

    
if __name__ == '__main__':
	start_time = datetime.strptime('2017-10-01', "%Y-%m-%d")
	end_time = datetime.strptime('2017-10-04', "%Y-%m-%d")
	[pool.apply(worker,(i,)) for i in tqdm(db.test.find({'tweet.date':{'$gt':start_time,'$lt':end_time}},{'_id':1,'tweet.text':1}))]
	pool.close()
	pool.join()