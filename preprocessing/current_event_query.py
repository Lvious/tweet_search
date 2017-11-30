from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from datetime import datetime,timedelta
from pprint import pprint
from tqdm import tqdm

import pymongo
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
from pymongo.errors import BulkWriteError
client = pymongo.MongoClient('localhost:27017')
db = client.tweet

def get_tfidf_top(texts,top=5):
	tfidf_tops = []
	count = CountVectorizer(stop_words='english')
	X_train_count = count.fit_transform(texts)
	tfidf = TfidfTransformer(use_idf=True)
	X_train_tfidf = tfidf.fit_transform(X_train_count)
	id2words = dict([(v,k) for k,v in count.vocabulary_.items()])
	for i in X_train_tfidf.toarray():
		tfidf_tops.append([id2words[wid] for wid in i.argsort()[-top:][::-1]])
	return tfidf_tops

def get_ner_w(ner_dict,trigger):
	where_who = []
	why_what = []
	for k,v in ner_dict.iteritems():
		if k in ['PERSON','LOCATION','ORGANIZATION','MISC']:
			where_who.extend([x.lower() for x in v])
		elif k in ['MONEY','NUMBER','PERCENT','DATE','TIME','DURATION','ORDINAL','SET']:
			why_what.extend([x.lower() for x in v])
		else:
			pass
	trigger = list(set(trigger) - (set(where_who+why_what) & set(trigger)))
	why_what = why_what + trigger
	where_who = ['"'+i+'"' for i in where_who]
	where_who = '('+' OR '.join(where_who)+')'
	why_what = ['"'+i+'"' for i in why_what] 
	why_what = '('+' OR '.join(why_what)+')'
	return where_who,why_what

def get_query_str(item,trigger):
	date = datetime.strptime(item['event']['date'],'%Y-%m-%d')
	since = (date+timedelta(days=-1)).strftime('%Y-%m-%d')
	until = (date+timedelta(days=1)).strftime('%Y-%m-%d')
	ner_dict = item['ie']['ner_dict']
	where_who,why_what = get_ner_w(ner_dict,trigger)
	return where_who+' '+why_what+' since:'+since+' until:'+until
	
if __name__ == '__main__':
	for type_ in range(1,11):
		texts = []
		for item in db.current_event.find({'type':type_},{'event':1}):
			texts.append(item['event']['title']+' '+item['event']['description'])
		triggers = get_tfidf_top(texts,top=5)
		ids = []
		query_strs = []
		for index,item in enumerate(db.current_event.find({'type':type_},{'_id':1,'event':1,'ie.ner_dict':1})):
			ids.append(item['_id'])
			query_strs.append(get_query_str(item,triggers[index]))
		requests = [UpdateOne({'_id': _id}, {'$set': {'query_str':query_strs[index]}}) for index,_id in tqdm(enumerate(ids))]
		try:
			result = db.current_event.bulk_write(requests)
			pprint(result.bulk_api_result)
		except BulkWriteError as bwe:
			pprint(bwe.details)
	client.close()
			#print item['event']
			#print get_query_str(item,triggers[index])
			