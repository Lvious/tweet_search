# -*- coding:utf8 -*-
import json
from Config import get_spider_config
_,db,r = get_spider_config()
q_list = ['(Kwangmyongsong OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2012-04-12 until:2012-04-14',
 '(North Korea OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2012-12-11 until:2012-12-13',
 '(North Korean OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2013-05-17 until:2013-05-19',
 '(North Korean OR Nodong OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2014-03-19 until:2014-03-21',
 '(North Korea OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2015-05-08 until:2015-05-10',
 '(North Korea OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2016-02-06 until:2016-02-08',
 '(North Korea OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2016-04-08 until:2016-04-10',
 '(North Korea OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2016-08-23 until:2016-08-25',
 '(North Korean OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2016-10-14 until:2016-10-16',
 '(North Korea OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2016-10-18 until:2016-10-20',
 '(North Korea OR the Sea of Japan OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2017-02-10 until:2017-02-12',
 '(North Korea OR Tongchang OR the Sea of Japan OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2017-03-05 until:2017-03-07',
 '(North Korea OR Sinpo OR the Sea of Japan OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2017-04-03 until:2017-04-05',
 '(North Korea OR Sinpo OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2017-04-14 until:2017-04-16',
 '(North Korea OR Pukchang OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2017-04-27 until:2017-04-29',
 '(North Korea OR Kusong OR the Sea of Japan OR Guam OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2017-05-12 until:2017-05-14',
 "(North Korea OR Pukkuksong OR Pukchang OR the Sea of Japan OR North Korea's OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2017-05-20 until:2017-05-22",
 '(North Korea OR the Sea of Japan OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2017-05-28 until:2017-05-30',
 '(North Korea OR the Sea of Japan OR South Korean OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2017-06-07 until:2017-06-09',
 '(North Korea OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2017-06-22 until:2017-06-24',
 '(North Korea OR ICBM OR Hwasong4 on OR Japan OR Alaska OR Hawaii OR Seattle OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2017-07-03 until:2017-07-05',
 '(North Korea OR ICBM OR Chagang Province OR Los Angeles OR Denver OR Chicago OR Boston OR New York OR RV OR Japan OR Japanese OR Hokkaido OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2017-07-27 until:2017-07-29',
 '(North Korea OR Kangwon OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2017-08-25 until:2017-08-27',
 '(North Korea OR Northern Japan OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2017-08-28 until:2017-08-30',
 '(North Korea OR Hokkaido OR Pacific OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2017-09-14 until:2017-09-16',
 '(North Korea OR ICBM OR Pyongsong OR Pyongyang OR the Sea of Japan OR Japanese OR Washington OR missile OR satellite OR rocket) (test OR tested OR fire OR fired OR launch OR launched) since:2017-11-27 until:2017-11-29']
event_id=range(1,len(q_list)+1)
def get_task():
    for q,id in zip(q_list,event_id):
        message = {'q':q,'f':['&f=news','','&f=tweets'],'num':100,'event_id':id}
        print message
        r.rpush('task:dataset',json.dumps(message))

if __name__ == '__main__':
    get_task()

