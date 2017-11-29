import urllib,urllib2,json,re,datetime,sys,cookielib
from .. import models
from pyquery import PyQuery
import random
random.seed(1)

from TweetManager import TweetManager,getTweet

def getDialog(original,screen_name,conversation_id,refreshCursor='', receiveBuffer=None, bufferLength=100, proxy=None):
	results = {}
	results['original'] = original.__dict__
	results['conversation'] = []
	resultsAux = []
	cookieJar = cookielib.CookieJar()
	
	active = True
	
	while active:
		json = DialogManager.getJsonReponse(screen_name,conversation_id, refreshCursor, cookieJar, proxy)
		if len(json['items_html'].strip()) == 0:
			break

		if not json.has_key('min_position'):
			break
		refreshCursor = json['min_position']

		items = PyQuery(json['items_html'])('ol.stream-items')
		
		if len(items) == 0:
			break
		
		for item in items:
			tweets = []
			for tweet in PyQuery(item)('div.js-stream-tweet'):
				tweets.append(getTweet(tweet).__dict__)
				
			results['conversation'].append(tweets)
			#resultsAux.append(tweets)
			
			if receiveBuffer and len(resultsAux) >= bufferLength:
				receiveBuffer(resultsAux)
				resultsAux = []
				
		if refreshCursor == None:
			break
			
	if receiveBuffer and len(resultsAux) > 0:
		receiveBuffer(resultsAux)
	
	return results
	
class DialogManager:
	
	def __init__(self):
		pass
		
	@staticmethod
	def getDialogById(tweet_id):
		tweet = TweetManager.getTweetsById(tweet_id)
		if not tweet.is_reply and tweet.action['replies'] > 0:
			return getDialog(tweet,tweet.user['screen_name'],tweet.conversation_id)
		elif tweet.is_reply:
			tweet = TweetManager.getTweetsById(tweet.conversation_id)
			return getDialog(tweet,tweet.user['screen_name'],tweet.conversation_id)
		else:
			return tweet.__dict__
			
	@staticmethod
	def getDialogs(tweetCriteria, refreshCursor='', receiveBuffer=None, bufferLength=100, proxy=None):
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
					if tweet.created_at < tweetCriteria.sinceTimeStamp:
						active = False
						break
				
				dialog = DialogManager.getDialogById(tweet.id)
				results.append(dialog)
				resultsAux.append(dialog)
				
				if receiveBuffer and len(resultsAux) >= bufferLength:
					receiveBuffer(resultsAux)
					resultsAux = []
				
				if tweetCriteria.maxTweets > 0 and len(results) >= tweetCriteria.maxTweets:
					active = False
					break
					
		
		if receiveBuffer and len(resultsAux) > 0:
			receiveBuffer(resultsAux)
		
		return results
		
	@staticmethod
	def getJsonReponse(screen_name,conversation_id, refreshCursor, cookieJar, proxy):
		url = 'https://twitter.com/i/%s/conversation/%s?include_available_features=1&include_entities=1&max_position=%s&reset_error_state=false'
		url = url % (screen_name,urllib.quote(conversation_id), refreshCursor)
		ua = 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.%s'%(random.randint(0,999))

		headers = [
			('Host', "twitter.com"),
			('User-Agent', ua), 
			# Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.81 Safari/537.36 
			#Mozilla/5.0 (Windows NT 6.1; Win64; x64)
			('Accept', "application/json, text/javascript, */*; q=0.01"),
			('Accept-Language', "de,en-US;q=0.7,en;q=0.3"),
			('X-Requested-With', "XMLHttpRequest"),
			('Referer', url),
			('Connection', "keep-alive")
		]

		if proxy:
			opener = urllib2.build_opener(urllib2.ProxyHandler({'http': proxy, 'https': proxy}), urllib2.HTTPCookieProcessor(cookieJar))
		else:
			opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cookieJar))
		opener.addheaders = headers

		try:
			response = opener.open(url)
			jsonResponse = response.read()
		except Exception,e:
			print "Twitter weird response. Try to see on browser:\n"+url
			raise Exception(e.message)
			#sys.exit()
			#return None
		
		dataJson = json.loads(jsonResponse)
		
		return dataJson		