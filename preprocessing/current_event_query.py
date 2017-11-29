


def get_ner_w(ner_dict):
	where_who = []
	why_what = []
	for k,v in ner_dict.iteritems():
		if k in ['PERSON','LOCATION','ORGANIZATION','MISC']:
			where_who.extend(v)
		elif k in ['MONEY','NUMBER','PERCENT','DATE','TIME','DURATION','ORDINAL','SET']:
			why_what.extend(v)
		else:
			pass
	where_who = ['"'+i+'"' for i in where_who]
	where_who = '('+' OR '.join(where_who)+')'
	why_what = ['"'+i+'"' for i in why_what] 
	why_what = '('+' OR '.join(why_what)+')'
	return where_who,why_what

def get_query_str(item):
	date = datetime.strptime(item['event']['date'],'%Y-%m-%d')
	since = (date+timedelta(days=-1)).strftime('%Y-%m-%d')
	until = (date+timedelta(days=1)).strftime('%Y-%m-%d')
	ner_dict = item['ie']['ner_dict']
	where_who,why_what = get_ner_w(ner_dict)
	return where_who+' '+why_what+' since:'+since+' until:'+until
	
		
for item in db.current_event.find({'type':2},{'event.date':1,'ie.ner_dict':1}).limit(15):
    print get_query_str(item)