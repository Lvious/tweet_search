#docker run -p 9000:9000 --name corenlp -i -t konradstrack/corenlp
#aliyun  934971
import re
from datetime import datetime,timedelta
from collections import Counter,defaultdict

from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.pipeline import Pipeline
from sklearn.cluster import MiniBatchKMeans
from sklearn.decomposition import LatentDirichletAllocation
from sklearn import metrics

from pprint import pprint
import pymongo
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
client = pymongo.MongoClient('34.224.37.110:27017')
db = client.tweet

from tqdm import tqdm

def tweet_cluster(X,max_cluster_num=8):
		count = CountVectorizer(stop_words='english')
		X_train_count = count.fit_transform(X)
		tfidf = TfidfTransformer(use_idf=True)
		X_train_tfidf = tfidf.fit_transform(X_train_count)
		km,score = kmeans_best(X,X_train_tfidf,max_cluster_num)
		#print 'clusters:',km.get_params()['n_clusters']
		lda = LatentDirichletAllocation(n_topics=km.get_params()['n_clusters'],
									max_iter=5,
									learning_method='online',
									learning_offset=50.,
									random_state=0)
		lda.fit(X_train_tfidf)
		tf_feature_names = count.get_feature_names()
		lda_words = get_topics_top_words(lda, tf_feature_names)
		return km,lda_words


def kmeans_best(X,X_train_tfidf=None,max_cluster_num=8):
	text_clusters = []
	for i in range(max_cluster_num)[2:]:
		#print i
		mbkm = MiniBatchKMeans(n_clusters=i)
		mbkm.fit(X_train_tfidf)
		socre = metrics.silhouette_score(X_train_tfidf, mbkm.labels_)
		text_clusters.append((mbkm,socre))
	return sorted(text_clusters,key=lambda i:i[1])[0]
    
def get_topics_top_words(model, feature_names, n_top_words=10):
	topics_top_words = {}
	for topic_idx, topic in enumerate(model.components_):
		topics_top_words[str(topic_idx)] = [feature_names[i] for i in topic.argsort()[:-n_top_words - 1:-1]]
	return topics_top_words
	
def get_ner(ner_list):
	who,where,when =([],[],[])
	for ner in ner_list:
		if ner.values[0] in ['PERSON','ORGANIZATION','MISC']:
			who.extend(ner.keys[0])
		elif ner.values[0] in ['LOCATION']:
			where.extend(ner.keys[0])
		elif ner.values[0] in ['DATE','TIME']:
			when.extend(ner.keys[0])
		else:
			pass
	# for k,v in ner_dict.iteritems():
		# if k in ['PERSON','ORGANIZATION','MISC']:
			# who.extend([x.lower() for x in v])
		# if k in ['LOCATION']:
			# where.extend([x.lower() for x in v])
		# elif k in ['DATE','TIME']:
			# when.extend([x.lower() for x in v])
		# else:
			# pass
	return who,where,when
	
	
if __name__ == '__main__':
	start_time = datetime.strptime('2017-10-01 00:00:00', "%Y-%m-%d %H:%M:%S")
	end_time = datetime.strptime('2017-10-03', "%Y-%m-%d")
	query = db.test.find({'tweet.date':{'$gte':start_time,'&lt':end_time},'class.1':{'$gte':0.5},'cluster':None},{'_id':1,'tweet.text':1,'tweet.hashtags':1,'tweet.date':1,'ie.ner':1})
	for batch in query.batch_size(1000)
		ids = []
		texts = []
		hashtags = []
		dates = []
		ners = []
		for i in batch:
			ids.append(i['_id'])
			texts.append(re.sub('http.+','',text.encode('utf-8')))
			hashtags.append(i['tweet']['hashtags'])
			dates.append(i['tweet']['date'])
			ners.append(i['ie']['ners'])
		if len(ids) == 0:
			continue
		cluster_hash = ids[0]+ids[-1]
		km,_ = tweet_cluster(texts)
		clusters = km.labels_
		clusters = [int(i) for i in clusters]
		requests = [UpdateOne({'_id': _id,'cluster':None}, {'$set': {'cluster':{'cluster_label':clusters[index],'cluster_hash':cluster_hash}}}) for index,_id in tqdm(enumerate(ids))]
		result = db.test.bulk_write(requests)
		pprint(result.bulk_api_result)
		
		whos,wheres,whens = (defaultdict(list),defaultdict(list),defaultdict(list))
		for index,_id in tqdm(enumerate(ids)):
			who,where,when = get_ner(ners[index])
			whos[clusters[index]].extend(who)
			wheres[clusters[index]].extend(where)
			whens[clusters[index]].extend(when)
		
		clusters_counter = dict(Counter(clusters))
		clusters_counter_ = defaultdict()
		for k,v in clusters_counter.iteritems():
			clusters_counter_[str(k)] = v
		
		cluster_entities = defaultdict()
		for k,v in clusters_counter.iteritems():
			cluster_entities[str(k)] = {'whos':whos[k]
																	'wheres':wheres[k]
																	'whens':whens[k]
			}
		
		clusters_hashtags = defaultdict(list)
		for index,hashtag in enumerate(hashtags):
			if len(hashtag) > 0:
				clusters_hashtags[str(clusters[index])].append(hashtag)
		clusters_hashtags_ = []
		for i in clusters_hashtags:
			clusters_hashtags_.extend(list(set(i.split(''))))
		clusters_hashtags_ = list(set(clusters_hashtags_))
		
		
		db.cluster_metadata.insert_one({'_id':cluster_hash,'start_time':dates[0],'end_time':dates[-1],'texts_num':len(texts),'clusters_size':clusters_counter_,'clusters_hashtags':clusters_hashtags_},'cluster_entities':cluster_entities)
		client.close()

# if __name__ == '__main__':
	# start_time = datetime.strptime('2017-10-01 00:00:00', "%Y-%m-%d %H:%M:%S")
	# hours_ = [(start_time + timedelta(hours=i)) for i in range(24*16)]
	# for hour in tqdm(hours_):
		# end_time = hour + timedelta(hours=1)
		#cluster_hash = hash(hour)+hash(end_time)
		# cluster_hash = str(hash(hour.strftime('%Y-%m-%d %H:%M:%S')+'~'+end_time.strftime('%Y-%m-%d %H:%M:%S')))
		# query = db.test.find({'tweet.date':{'$gte':hour,'$lt':end_time},'class.1':{'$gte':0.5},'cluster':None},{'_id':1,'tweet.text':1,'tweet.hashtags':1})
		# ids = []
		# texts = []
		# hashtags = []
		# for i in query:
			# ids.append(i['_id'])
			# texts.append(i['tweet']['text'])
			# hashtags.append(i['tweet']['hashtags'])
		# if len(ids) == 0:
			# continue
		# km,lda_words = tweet_cluster(texts)
		# clusters = km.labels_
		# clusters = [int(i) for i in clusters]
		# requests = [UpdateOne({'_id': _id,'cluster':None}, {'$set': {'cluster':{'cluster_label':clusters[index],'cluster_hash':cluster_hash}}}) for index,_id in tqdm(enumerate(ids))]
		# result = db.test.bulk_write(requests)
		# pprint(result.bulk_api_result)
		# clusters_counter = dict(Counter(clusters))
		# clusters_counter_ = defaultdict()
		# for k,v in clusters_counter.iteritems():
			# clusters_counter_[str(k)] = v
		# clusters_hashtags = defaultdict(list)
		# for index,hashtag in enumerate(hashtags):
			# if len(hashtag) > 0:
				# clusters_hashtags[str(clusters[index])].append(hashtag)
		# db.cluster_metadata.insert_one({'_id':cluster_hash,'start_time':hour,'end_time':end_time,'texts_num':len(texts),'clusters_size':clusters_counter_,'clusters_hashtags':clusters_hashtags,'topics':lda_words})
		# client.close()