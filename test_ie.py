from pycorenlp import StanfordCoreNLP
nlp = StanfordCoreNLP('http://101.132.182.124:9000')

from pprint import pprint
from tqdm import tqdm

import pymongo
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
client = pymongo.MongoClient('34.224.37.110:27017')
db = client.tweet

def get_ner_openie_sentiment(text):
	output = nlp.annotate(text, properties={
						  'annotators': 'ner,sentiment,openie',
						  'outputFormat': 'json',
						  })
	for i in output['sentences'][0]['openie']:
		temp = i.pop('objectSpan')
		temp = i.pop('relationSpan')
		temp = i.pop('subjectSpan')
	ners = []
	for i in output['sentences'][0]['tokens']:
		if i['ner'] != u'O':
			ners.append({i['word']:i['ner']})
	return {
		'sentiment':output['sentences'][0]['sentiment'],
		'sentimentValue':output['sentences'][0]['sentimentValue'],
		'openie':output['sentences'][0]['openie'],
		'ner':ners,
	}

def batch_ie(texts)
	ies = []
	for text in tqdm(texts)£º
		ies.append(get_ner_openie_sentiment(text))
	return ies
	
if __name__ == '__main__':  #bulk_write
	start_time = datetime.strptime('2017-10-01', "%Y-%m-%d")
	end_time = datetime.strptime('2017-10-04', "%Y-%m-%d")
	query = db.test.find({'tweet.date':{'$gt':start_time,'$lt':end_time},'ie':None},{'_id':1,'tweet.text':1})
	ids = []
	texts = []
	for i in query:
		ids.append(i['_id'])
		texts.append(i['tweet']['text'])
	ies = batch_ie(texts)
	requests = [UpdateOne({'_id': _id,'ie':None}, {'$set': {'ie':ies[index]}}) for index,_id in tqdm(enumerate(ids))]
	result = db.test.bulk_write(requests)
	pprint(result.bulk_api_result)
	client.close()