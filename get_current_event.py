import re
from tqdm import tqdm

from bs4 import BeautifulSoup
import requests
headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.81 Safari/537.36'}

from pprint import pprint
import pymongo
from pymongo import InsertOne
client = pymongo.MongoClient('101.132.182.124:27027')
db = client.tweet

Portal_url = 'https://en.wikipedia.org/wiki/Portal:Current_events/'
years = [2010+i for i in range(8)]
months = ["January","February","March","April","May","June","July","August","September","October","November","December"]
page_urls = [Portal_url+m+'_'+str(y) for y in years for m in months][6:]

def trans_date(date_str):
	month = date_str.split('_')[1]
	month_num = months.index(month)+1
	date_str = date_str.replace(month,'0'+str(month_num) if month_num<10 else str(month_num))
	date_str = date_str.replace('_','-')
	return date_str

def get_events_from_page(page_url):
	res = requests.get(page_url,headers=headers)
	soup = BeautifulSoup(res.text,'lxml')
	tables = soup.find_all('table',attrs={'class':'vevent'})
	events = []
	for t in tables:
		date = t.find_previous_sibling().get('id')
		td = t.find('td',attrs={'class':'description'})
		for type_,ul in zip([dl.get_text() for dl in td.find_all('dl')],[ul for ul in td.find_all('ul',recursive=False)]):
			for li in ul.find_all('li',recursive=False):
				try:
					events.append({'date':trans_date(date),
								   'class':type_.strip(),
								   'title':li.a.get_text(),
								   'description':li.ul.get_text().strip()})
				except:
					pass
	return events
	
if __name__ == '__main__':
	event_dicts = []
	for page_url in tqdm(page_urls):
		event_dicts.extend(get_events_from_page(page_url))
	requests_ = [InsertOne({'_id': hash(i['date']+i['title']),'event':i}) for i in tqdm(event_dicts)]
	result = db.current_event.bulk_write(requests_)
	print(result.bulk_api_result)