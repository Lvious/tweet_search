import urllib,urllib2,json,re,datetime,sys,cookielib
from .. import models
from pyquery import PyQuery
import requests
import random
random.seed(1)

def fetch_activities(tweet_id):
	retusers = []
	favorusers = []
	re_url = 'https://twitter.com/i/activity/retweeted_popup?id=%s'%(tweet_id)
	favor_url = 'https://twitter.com/i/activity/favorited_popup?id=%s'%(tweet_id)
	headers = {
			'Host':"twitter.com",
			'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.%s'%(random.randint(0,999)),
			'Accept':"application/json, text/javascript, */*; q=0.01",
			'Accept-Language':"de,en-US;q=0.7,en;q=0.3",
			'X-Requested-With':"XMLHttpRequest",
			'Referer':'https://twitter.com/',
			'Connection':"keep-alive",
		}
	re_users = PyQuery(requests.get(re_url,headers=headers).json()['htmlUsers'])('ol.activity-popup-users')
	for re_user in re_users('div.account'):
		retusers.append(PyQuery(re_user).attr('data-screen-name'))
	favor_users = PyQuery(requests.get(favor_url,headers=headers).json()['htmlUsers'])('ol.activity-popup-users')
	for favor_user in favor_users('div.account'):
		favorusers.append(PyQuery(favor_user).attr('data-screen-name'))
		
	return retusers,favorusers

def fetch_entities(tweetPQ):
	hashtags = []
	urls = []
	for url in tweetPQ('p.js-tweet-text a'):
		d = dict(url.items())
		if d.has_key('data-expanded-url'): #d['class'] == 'twitter-timeline-link' 
			#pdb.set_trace()
			urls.append({'href':d['href'],'expanded_url':d['data-expanded-url']})
		if d['href'].startswith('/hashtag/'):
			hashtags.append(d['href'].split('?')[0].split('/')[-1])
	tweetPQ('p.js-tweet-text a.twitter-timeline-link').remove()
	return hashtags,urls

def getTweet(tweetHTML):
	tweetPQ = PyQuery(tweetHTML)
	tweet = models.Tweet()
	
	#base info
	id = tweetPQ.attr("data-tweet-id")
	retweet_id = tweetPQ.attr('data-retweet-id')
	retweeter = tweetPQ.attr('data-retweeter')
	conversation_id = tweetPQ.attr('data-conversation-id')
	#permalink = tweetPQ.attr("data-permalink-path")
	dateSec = int(tweetPQ("small.time span.js-short-timestamp").attr("data-time"))
	
	#user
	user_screen_name = tweetPQ.attr('data-screen-name')
	user_id = tweetPQ.attr('data-user-id')
	
	#text
	hashtags,urls = fetch_entities(tweetPQ)
	retusers,favorusers = fetch_activities(id)
	mentions = tweetPQ.attr("data-mentions")
	lang = tweetPQ("p.js-tweet-text").attr('lang')
	raw_text = re.sub(r"\s+", " ", tweetPQ("p.js-tweet-text").text().replace('# ', '#').replace('@ ', '@'))
	standard_text = re.sub(r"\s+", " ", tweetPQ("p.js-tweet-text").text().replace('# ', '').replace('@ ', ''))
	tweetPQ('p.js-tweet-text')('a').remove()
	tweetPQ('p.js-tweet-text')('img').remove()
	clean_text = tweetPQ("p.js-tweet-text").text()
	
	#media
	quote_id = tweetPQ('div.QuoteTweet a.QuoteTweet-link').attr('data-conversation-id')
	has_cards = tweetPQ.attr('data-has-cards')
	card_url = tweetPQ('div.js-macaw-cards-iframe-container').attr('data-card-url')
	img_src = tweetPQ('div.AdaptiveMedia-container img').attr('src')
	video_src = tweetPQ('div.AdaptiveMedia-container video').attr('src')
	
	#count
	replies = int(tweetPQ("span.ProfileTweet-action--reply span.ProfileTweet-actionCount").attr("data-tweet-stat-count").replace(",", ""))
	retweets = int(tweetPQ("span.ProfileTweet-action--retweet span.ProfileTweet-actionCount").attr("data-tweet-stat-count").replace(",", ""))
	favorites = int(tweetPQ("span.ProfileTweet-action--favorite span.ProfileTweet-actionCount").attr("data-tweet-stat-count").replace(",", ""))

	#geo
	geo = ''
	geoSpan = tweetPQ('span.Tweet-geo')
	if len(geoSpan) > 0:
		geo = geoSpan.attr('title')
	
	## tweet model
	
	tweet.id = id
	tweet.retweet_id = retweet_id
	tweet.retweeter = retweeter
	tweet.is_retweet = True if tweet.retweet_id != None else False
	tweet.conversation_id = conversation_id
	tweet.is_reply = tweet.id != tweet.conversation_id
	#tweet.permalink = 'https://twitter.com' + permalink
	tweet.created_at = datetime.datetime.fromtimestamp(dateSec)
	
	tweet.username = user_screen_name
	tweet.uid = user_id
	
	tweet.quote_id = quote_id
	tweet.has_cards = has_cards
	tweet.card_url = card_url
	tweet.img_src = img_src
	tweet.video_src = video_src
	
	tweet.hashtags = hashtags
	tweet.urls = urls
	tweet.retusers = retusers
	tweet.favorusers = favorusers
	tweet.mentions = mentions.split(' ') if mentions != None else None
	tweet.lang = lang
	tweet.raw_text = raw_text
	tweet.standard_text = standard_text
	#tweet.clean_text = clean_text
	
	tweet.replies = replies
	tweet.retweets = retweets
	tweet.favorites = favorites

	tweet.geo = geo
	return tweet

class TweetManager:
	
	def __init__(self):
		pass
		
	@staticmethod
	def getTweetsById(tweet_id):
		url = 'https://twitter.com/xxx/status/%s'%(tweet_id)
		headers = {
			'Host':"twitter.com",
			'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.%s'%(random.randint(0,999)),
			'Accept':"application/json, text/javascript, */*; q=0.01",
			'Accept-Language':"de,en-US;q=0.7,en;q=0.3",
			'X-Requested-With':"XMLHttpRequest",
			'Referer':'https://twitter.com/',
			'Connection':"keep-alive",
		}
		tweets = PyQuery(requests.get(url,headers=headers).content)('div.js-original-tweet')
		for tweetHTML in tweets:
			return getTweet(tweetHTML)
		
	@staticmethod
	def getTweets(tweetCriteria, refreshCursor='', receiveBuffer=None, bufferLength=100, proxy=None):
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
			
			if not json.has_key('min_position'):
				break
			refreshCursor = json['min_position']
			if refreshCursor == None:
				break
			tweets = PyQuery(json['items_html'])('div.js-stream-tweet')
			
			if len(tweets) == 0:
				break
			
			for tweetHTML in tweets:
				tweet = getTweet(tweetHTML)
				if hasattr(tweetCriteria, 'sinceTimeStamp'):
					if tweet.created_at < tweetCriteria.sinceTimeStamp:
						active = False
						break
				
				results.append(tweet)
				#resultsAux.append(tweet)
				
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
	def getJsonReponse(tweetCriteria, refreshCursor, cookieJar, proxy):
		url = "https://twitter.com/i/search/timeline?q=%s&src=typd&max_position=%s"
		
		urlGetData = ''
		
		if hasattr(tweetCriteria, 'username'):
			urlGetData += ' from:' + tweetCriteria.username
		
		if hasattr(tweetCriteria, 'querySearch'):
			urlGetData += ' ' + tweetCriteria.querySearch
		
		if hasattr(tweetCriteria, 'near'):
			urlGetData += "&near:" + tweetCriteria.near + " within:" + tweetCriteria.within
		
		if hasattr(tweetCriteria, 'since'):
			urlGetData += ' since:' + tweetCriteria.since
			
		if hasattr(tweetCriteria, 'until'):
			urlGetData += ' until:' + tweetCriteria.until
		

		if hasattr(tweetCriteria, 'topTweets'):
			if tweetCriteria.topTweets:
				url = "https://twitter.com/i/search/timeline?q=%s&src=typd&max_position=%s"
		
		if hasattr(tweetCriteria, 'tweetType'):
			url = url + tweetCriteria.tweetType
		
		url = url % (urllib.quote(urlGetData), refreshCursor)
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
			print "Twitter weird response. Try to see on browser: https://twitter.com/search?q=%s&src=typd" % urllib.quote(urlGetData)
			raise Exception(e.message)
			#sys.exit()
			#return None
		
		dataJson = json.loads(jsonResponse)
		
		return dataJson		
