from datetime import datetime,timedelta

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
		lda = LatentDirichletAllocation(n_topics=km.get_params()['n_clusters'], max_iter=5,
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
  
if __name__ == '__main__':
	start_time = datetime.strptime('2017-10-01 00:00:00', "%Y-%m-%d %H:%M:%S")
	hours_ = [(start_time + timedelta(hours=i)) for i in range(72)]
	for hour in tqdm(hours_):
		end_time = hour + timedelta(hours=1)
		cluster_hash = hash(hour)+hash(end_time)
		query = db.test.find({'tweet.date':{'$gt':hour,'$lt':end_time},'cluster':None},{'_id':1,'tweet.text':1})
		ids = []
		texts = []
		for i in query:
			ids.append(i['_id'])
			texts.append(i['tweet']['text'])
		if len(ids) == 0:
			continue
		km,lda_words = tweet_cluster(texts)
		clusters = km.labels_
		clusters = [int(i) for i in clusters]
		requests = [UpdateOne({'_id': _id,'cluster':None}, {'$set': {'cluster':{'cluster_label':clusters[index],'cluster_hash':cluster_hash}}}) for index,_id in tqdm(enumerate(ids))]
		result = db.test.bulk_write(requests)
		pprint(result.bulk_api_result)
		db.cluster_metadata.insert_one({'_id':cluster_hash,'start_time':hour,'end_time':end_time,'topics':lda_words})