import urllib,urllib2,json,re,datetime,sys,cookielib
from .. import models
from pyquery import PyQuery
import random
random.seed(1)

class UserManager:
	'''
	from:xxx + to:xxx = @xxx
	'''
	def __init__(self):
		pass
		
	@staticmethod
	def get_base_info(tweet_id):
		url = 'https://twitter.com/xxx/status/%s'%(tweet_id)
		tweets = PyQuery(url)('div.js-original-tweet')
		for tweetHTML in tweets:
			return getTweet(tweetHTML)
		
	@staticmethod
	def getTweets(tweetCriteria, refreshCursor='', bulk_write_num=1000, receiveBuffer=None, bufferLength=100, proxy=None):
		bulk_write_index = 0
		results = []
		resultsAux = []
		cookieJar = cookielib.CookieJar()
		
		if hasattr(tweetCriteria, 'username') and (tweetCriteria.username.startswith("\'") or tweetCriteria.username.startswith("\"")) and (tweetCriteria.username.endswith("\'") or tweetCriteria.username.endswith("\"")):
			tweetCriteria.username = tweetCriteria.username[1:-1]

		active = True

		while active:
			json = TweetManager.getJsonReponse(tweetCriteria, refreshCursor, cookieJar, proxy)
			if len(json['items_html'].strip()) == 0:
				break

			refreshCursor = json['min_position']
			tweets = PyQuery(json['items_html'])('div.js-stream-tweet')
			
			if len(tweets) == 0:
				break
			
			for tweetHTML in tweets:
				tweet = getTweet(tweetHTML)
				if hasattr(tweetCriteria, 'sinceTimeStamp'):
					if tweet.date < tweetCriteria.sinceTimeStamp:
						active = False
						break
				
				results.append(tweet)
				if len(results[bulk_write_index:bulk_write_index+bulk_write_num]) > bulk_write_num:
					
					bulk_write_index += bulk_write_num
				resultsAux.append(tweet)
				
				if receiveBuffer and len(resultsAux) >= bufferLength:
					receiveBuffer(resultsAux)
					resultsAux = []
				
				if tweetCriteria.maxTweets > 0 and len(results) >= tweetCriteria.maxTweets:
					active = False
					break
					
		
		if receiveBuffer and len(resultsAux) > 0:
			receiveBuffer(resultsAux)
		
		return results
