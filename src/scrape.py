# python src/scrape.py --after 1000

import argparse
import requests
import random
import os
import thread
import time
import redis
 
from constant import user_agents
from scrape_proxies import ProxyService
from utils import get_connection, retry, load_ids_set

AMAZON_APP_URL = 'http://www.amazon.com/dp/{app_id}'
PAGE_PATH = 'res/{date}/{app_id}.txt'
DIR_PATH = 'res/{date}'
APP_PAGE_SIZE = 10000
error_proxies = dict()
APP_SOURCE_PAGE_KEY = 'amazon-app-detail/{date}/{app_id}'


class AmazonAppSpider(object):
    def __init__(self):
        self.proxy_service = ProxyService()
	self.redis_obj = redis.StrictRedis(host='localhost', port=6379, db=0) 

    def scrape(self, date_str, app_ids_list):
        thread.start_new_thread(self._load_proxies, (600, ))

        for app_id in app_ids_list:
            app_page_key = APP_SOURCE_PAGE_KEY.format(date=date_str, app_id=app_id)
	    if self.redis_obj.exists(app_page_key):
                continue
            try:
                self._scrape(app_id, app_page_key)
            except:
                print 'Failed scrape app', app_id

    @retry(5)
    def _scrape(self, app_id, app_page_key):
        scrape_url = AMAZON_APP_URL.format(app_id=app_id)
        header = {'content-type': 'text/html',
                  'User-Agent': user_agents[random.randint(0, len(user_agents)-1)]}
        proxy = self.proxy_service.get_proxy()
        try:
            response = requests.get(scrape_url, timeout=30, headers=header, proxies=proxy)
            if len(response.content) > APP_PAGE_SIZE:
                self._save_page(app_page_key, response.content)
                self.proxy_service.manage(proxy, False)
           	print 'Succeed scrape app', app_id
	    else:
                raise
        except:
            self.proxy_service.manage(proxy, True)
            raise Exception('Failed scrape app page')

    def _save_page(self, app_page_key, content):
        self.redis_obj.set(app_page_key, content)

    def _load_proxies(self, delay):
        while True:
            self.proxy_service.load_proxies('proxies.csv')
            print 'Succeed load proxies.'
            time.sleep(delay)


if __name__ == '__main__':
    par = argparse.ArgumentParser()
    par.add_argument(
        '--after', dest='after', type=int, required=True,
        help='Name of file'
    )
    par.add_argument(
        '--date', dest='date', type=str, required=True,
        help='Name of file'
    )
    args = par.parse_args()

    db = get_connection()
    app_ids_list = list(load_ids_set(db))[args.after:]

    amazon_app_spider = AmazonAppSpider()
    amazon_app_spider.scrape(args.date, app_ids_list)
    db.close()
    print 'Succeed to finish.'
