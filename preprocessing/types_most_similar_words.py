import pandas as pd
import gensim

wv_wiki_dir = u'C:/Users/lxp/Desktop/nlp/word2vec/glove.840B.300d.word2vec.txt'
wv_wiki = gensim.models.KeyedVectors.load_word2vec_format(wv_wiki_dir,binary=False)

wv_tweet_dir = u'C:/Users/lxp/Desktop/nlp/word2vec/gensim.glove.twitter.27B.200d.txt'
wv_tweet = gensim.models.KeyedVectors.load_word2vec_format(wv_tweet_dir,binary=False)

types = {
				'1':[['army','attack'],['conflict']],
				'2':[['disaster'],['accident']],
				'3':[['law'],['crime']],
				'4':[['political'],['election']],
				'5':[['international','relation']],
				'6':[['science'],['technology']],
				'7':[['business'],['economic']],
				'8':[['art'],['culture']],
				'9':[['sport']],
				'10':[['health',],['environment']],
}

def get_type_most_similar(k,v,wv_wiki,wv_tweet,topn=3000):
	ms = []
	for i in v:
		ms.extend(wv_wiki.most_similar(positive=i,topn=topn))
		ms.extend(wv_tweet.most_similar(positive=i,topn=topn))
	df = pd.DataFrame.from_records(ms,index=range(len(ms)),columns=['word','similarity'])
	df.sort_values(by='similarity',ascending=False)
	df.drop_duplicates(['word'])
	df.to_csv(k+'.csv',sep=',',encoding='utf-8',index=False)
	
if __name__ == '__main__':
	for k,v in types.iteritems():
		get_type_most_similar(k,v,wv_wiki,wv_tweet)
		break