from pycorenlp import StanfordCoreNLP
nlp = StanfordCoreNLP('http://101.132.182.124:9000')

from datetime import datetime,timedelta
from pprint import pprint
from tqdm import tqdm
import re

import pymongo
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
from pymongo.errors import BulkWriteError
client = pymongo.MongoClient('34.224.37.110:27017')
db = client.tweet

def get_ner_list(word,ner):
	ner_tuple = zip(word,ner)
	splits = [0]
	for index,nt in enumerate(ner_tuple):
		if index == 0:
			temp = nt[1]
			continue
		if temp != nt[1]:
			splits.append(index)
			temp = nt[1]
			continue
		else:
			temp = nt[1]
			continue
	ner_tuple_ = []
	for index,s in enumerate(splits):
		nt = ner_tuple[splits[index-1]:s]
		if len(nt) == 0:
			continue
		if nt[0][1] != u'O':
			ner_tuple_.append({' '.join([i[0] for i in nt]):nt[0][1]})
	return ner_tuple_

def ie_pipeline(text):
	output = nlp.annotate(text, properties={
						  'annotators': 'truecase,ner,sentiment,openie',
						  'outputFormat': 'json',
						  })
	sentences = output['sentences']
	if len(sentences) == 0:
		return {
		'word':{},
		'truecaseText':{},
		'lemma':{},
		'pos':{},
		'ner':{},
		'ner_list':{},
		'openie':{},
		'sentiment':{},
				}
	words = []
	truecaseTexts = []
	lemmas = []
	poss = []
	ners = []
	openies = []
	sentiments = []
	for sentence in sentences:
		tokens_df = pd.DataFrame.from_dict(sentence['tokens'])
		words.extend(tokens_df['word'].tolist())
		truecaseTexts.extend(tokens_df['truecaseText'].tolist())
		lemmas.extend(tokens_df['lemma'].tolist())
		poss.extend(tokens_df['pos'].tolist())
		ners.extend(tokens_df['ner'].tolist())
		for i in sentence['openie']:
			temp = i.pop('objectSpan')
			temp = i.pop('relationSpan')
			temp = i.pop('subjectSpan')
		openies.extend(sentence['openie'])
		sentiments.append((sentence['sentiment'],sentence['sentimentValue']))
	ner_list = get_ner_list(words,ners)
	return {
		'word':words,
		'truecaseText':truecaseTexts,
		'lemma':lemmas,
		'pos':poss,
		'ner':ners,
		'ner_list':ner_list,
		'openie':openies,
		'sentiment':sentiments,
	}

def batch_ie(texts):
	ies = []
	for text in tqdm(texts):
		ies.append(ie_pipeline(re.sub('http.+','',text.encode('utf-8'))))
	return ies
	
if __name__ == '__main__':  #bulk_write
	start_time = datetime.strptime('2017-10-01', "%Y-%m-%d")
	end_time = datetime.strptime('2017-10-04', "%Y-%m-%d")
	query = db.test.find({'tweet.date':{'$gte':start_time,'$lt':end_time},'class.1':{'$gte':0.5},'ie':None},{'_id':1,'tweet.text':1})
	ids = []
	texts = []
	for i in query:
		ids.append(i['_id'])
		texts.append(i['tweet']['text'])
	ies = batch_ie(texts)
	requests = [UpdateOne({'_id': _id,'ie':None}, {'$set': {'ie':ies[index]}}) for index,_id in tqdm(enumerate(ids))]
	#result = db.test.bulk_write(requests)
	try:
		db.test.bulk_write(requests)
	except BulkWriteError as bwe:
		pprint(bwe.details)
	pprint(result.bulk_api_result)
	client.close()