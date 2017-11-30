from pycorenlp import StanfordCoreNLP
nlp = StanfordCoreNLP('http://localhost:9000')

from datetime import datetime,timedelta
from collections import defaultdict,Counter
from pprint import pprint
from tqdm import tqdm
import re

import pymongo
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
from pymongo.errors import BulkWriteError
client = pymongo.MongoClient('localhost:27017')
db = client.tweet

import pandas as pd

def get_ner_dict(word,ner):
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
	ner_dict = defaultdict(list)
	for index,s in enumerate(splits):
		nt = ner_tuple[splits[index-1]:s]
		if len(nt) == 0:
			continue
		if nt[0][1] != u'O':
			ner_dict[nt[0][1]].append(' '.join([i[0] for i in nt]))
	ner_dict = dict(ner_dict)
	for k,v in ner_dict.iteritems():
		ner_dict[k] = list(set(v))
	return ner_dict
	
def get_most_common(items):
	count = Counter(items)
	return count.most_common()[0][0]
	
def best_openie(openies):
	if len(openies) == 0:
		return {}
	openie_df = pd.DataFrame.from_dict(openies)
	object_lsit = openie_df['object'].tolist()
	object_,lenth  = object_lsit[0],len(object_lsit[0])
	for i in object_lsit[1:]:
		if len(i) >= lenth:          # get the longest object
			object_,lenth = i,len(i)
	return {'subject':get_most_common(openie_df.subject),
			'relation':get_most_common(openie_df.relation),
			'object':object_}
	
def ie_pipeline(text):
	output = nlp.annotate(text, properties={
						  'annotators': 'truecase,ner,openie',#sentiment
						  'outputFormat': 'json',
						  })
	sentences = output['sentences']
	if len(sentences) == 0:
		return {
		#'word':{},
		#'truecaseText':{},
		#'lemma':{},
		#'pos':{},
		#'ner':{},
		'ner_list':{},
		'openie':{},
		#'sentiment':{},
				}
	words = []
	#truecaseTexts = []
	#lemmas = []
	#poss = []
	ners = []
	openies = []
	#sentiments = []
	for sentence in sentences:
		tokens_df = pd.DataFrame.from_dict(sentence['tokens'])
		words.extend(tokens_df['word'].tolist())
		#truecaseTexts.extend(tokens_df['truecaseText'].tolist())
		#lemmas.extend(tokens_df['lemma'].tolist())
		#poss.extend(tokens_df['pos'].tolist())
		ners.extend(tokens_df['ner'].tolist())
		for i in sentence['openie']:
			temp = i.pop('objectSpan')
			temp = i.pop('relationSpan')
			temp = i.pop('subjectSpan')
		openies.extend(sentence['openie'])
		#sentiments.append((sentence['sentiment'],sentence['sentimentValue']))
	ner_dict = get_ner_dict(words,ners)
	return {
		#'word':words,
		#'truecaseText':truecaseTexts,
		#'lemma':lemmas,
		#'pos':poss,
		#'ner':ners,
		'ner_dict':ner_dict,
		'openie':best_openie(openies),
		#'sentiment':sentiments,
	}

def batch_ie(texts):
	ies = []
	for text in tqdm(texts):
		ies.append(ie_pipeline(re.sub('http.+','',text.encode('utf-8'))))
	return ies
	
def clear_description(des):
	return re.sub('\([\w ]+\)','',des).strip()
	
if __name__ == '__main__':
	query = db.current_event.find({'ie':None},{'_id':1,'event.title':1,'event.description':1})
	ids = []
	texts = []
	for i in query:
		ids.append(i['_id'])
		texts.append(i['event']['title']+'. '+clear_description(i['event']['description']))
	ies = batch_ie(texts)
	requests = [UpdateOne({'_id': _id,'ie':None}, {'$set': {'ie':ies[index]}}) for index,_id in tqdm(enumerate(ids))]
	try:
		result = db.current_event.bulk_write(requests)
		pprint(result.bulk_api_result)
	except BulkWriteError as bwe:
		pprint(bwe.details)
	client.close()