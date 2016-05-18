# python src/scrape.py --after 1000

import argparse
import requests
import random
import os
import thread
import time
import redis
from multiprocessing import Process
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from copy import deepcopy

from constant import user_agents
from scrape_proxies import ProxyService
from utils import get_logger, get_connection, retry, load_ids_set
from constant import APP_SOURCE_PAGE_KEY 

AMAZON_APP_URL = 'http://www.amazon.com/dp/{app_id}'
REJECT_PAGE_SIZE = 10000 # 10K
NORMAL_APP_PAGE_SIZE = 100000 # 100K

logger = get_logger('scrap.log', __name__)


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
	    except Exception as ex:
                print 'Failed scrape and save app', app_id
		logger.exception(ex)
		logger.info('Failed scrape and save app {}'.format(app_id))

    @retry(2)
    def _scrape(self, app_id, app_page_key):
        scrape_url = AMAZON_APP_URL.format(app_id=app_id)
        header = {'content-type': 'text/html',
                  'User-Agent': user_agents[random.randint(0, len(user_agents)-1)]}
        proxy = self.proxy_service.get_proxy()
	try:
	    req = requests.Session()
	    retries = Retry(total=2, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
	    req.mount('http://', HTTPAdapter(max_retries=retries))
	    response = req.get(scrape_url, timeout=80, headers=header, proxies=proxy)
	    if len(response.content) > REJECT_PAGE_SIZE:
	        if len(response.content) > NORMAL_APP_PAGE_SIZE:
    		    print 'Succeed scrape app', app_id
		    self._save_page(app_page_key, response.content)
                    self.proxy_service.manage(proxy, False)
           	    print 'Succeed save redis app', app_id
		    logger.info('Succeed scrape and save app {}'.format(app_id))
	        else:
           	    print 'Invalid app', app_id
		    logger.info('Invalid app {}'.format(app_id))
	    else:
                raise Exception('Failed visit app page')
	
        except Exception as ex:
            self.proxy_service.manage(proxy, True)
            raise ex 

    def _save_page(self, app_page_key, content):
        self.redis_obj.set(app_page_key, content)

    def _load_proxies(self, delay):
        logger.info('Start thread to load proxy.')
	while True:
	    self.proxy_service.load_proxies('proxies.csv')
	    size = self.proxy_service.get_valid_size()
	    print 'Succeed load proxies. Valid size:', size
	    logger.info('Succeed load proxies. Valid size {}'.format(size))
	    time.sleep(delay)


def multi_scrape(i, date, ids):
    print 'Start process', i
    logger.info('Start process {}, need to scrape {} apps'.format(i, len(ids)))
    amazon_app_spider = AmazonAppSpider()
    amazon_app_spider.scrape(date, ids)
    print 'Succeed finish process', i
    logger.info('Succeed finish process {}'.format(i))


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
    par.add_argument(
        '--batch', dest='batch', type=int, default=1,
        help='Batch of ids'
    )
    args = par.parse_args()

    db = get_connection()
    app_ids_list = list(load_ids_set(db))[args.after:]
    db.close()
    batch_size = len(app_ids_list) / args.batch + 1

    process_list = list()
    for i in range(0, args.batch):
	app_ids = app_ids_list[i * batch_size: (i+1) * batch_size]
	process_list.append(Process(target=multi_scrape, args=(i, args.date, app_ids)))
	process_list[-1].start()

    for p in process_list:
	p.join()

    print 'Succeed to finish.'
    logger.info('Succeed to finish')
