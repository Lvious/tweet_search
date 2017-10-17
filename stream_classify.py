import os
import re
import time
import codecs 
import numpy as np
import subprocess
import multiprocessing
from multiprocessing.dummy import Pool

import pymongo
client = pymongo.MongoClient("localhost", 27017)
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
                        '4'], 
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
    print i['stream']['id']
    print i['stream']['timestamp']
    probs = ftpredict(i['stream']['tweet'])
    print probs
    db.stream.find_one_and_update({'_id': i['_id'],'probs':None}, { '$set':{'probs':probs)}})
    print 'done!'

    
if __name__ == '__main__':
    while True:
        pool = Pool(1)
        latest = db.time.find_one({'name':'stream_score_entropy'})['time']
        latest_stream = db.stream.find_one({'stream.timestamp':{'$gt':latest}},sort=[('stream.timestamp',pymongo.DESCENDING)])
        if latest_stream != None:
            db.time.find_one_and_update({'name':'stream_score_entropy'},{'$set':{'time':latest_stream['stream']['timestamp']}})
            [pool.apply(worker,(i,)) for i in db.stream.find({'stream.timestamp':{'$gt':latest}},sort=[('stream.timestamp',pymongo.DESCENDING)])]
            pool.close()
            pool.join()
            time.sleep(60*60)
        else:
            time.sleep(5*60)