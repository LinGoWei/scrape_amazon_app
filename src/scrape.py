# python src/scrape.py --after 1000

import argparse
import requests
import random
import os
import thread
import time

from constant import user_agents
from scrape_proxies import ProxyService
from utils import get_connection, retry, load_ids_set

AMAZON_APP_URL = 'http://www.amazon.com/dp/{app_id}'
PAGE_PATH = 'res/{date}/{app_id}.txt'
DIR_PATH = 'res/{date}'
APP_PAGE_SIZE = 10000
error_proxies = dict()


class AmazonAppSpider(object):
    def __init__(self):
        self.proxy_service = ProxyService()

    def scrape(self, date_str, app_ids_list):
        thread.start_new_thread(self._load_proxies, (600, ))

        dir_path = DIR_PATH.format(date=date_str)
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)

        for app_id in app_ids_list:
            print 'Started scrape', app_id
            if os.path.exists(PAGE_PATH.format(date=date_str, app_id=app_id)):
                continue
            try:
                self._scrape(date_str, app_id)
            except:
                print 'Failed scrape', app_id

    @retry(3)
    def _scrape(self, date_str, app_id):
        scrape_url = AMAZON_APP_URL.format(app_id=app_id)
        header = {'content-type': 'text/html',
                  'User-Agent': user_agents[random.randint(0, len(user_agents)-1)]}
        proxy = self.proxy_service.get_proxy()
        try:
            response = requests.get(scrape_url, timeout=30, headers=header, proxies=proxy)
            if len(response.content) > APP_PAGE_SIZE:
                print 'Succeed to scrape app page.'
                self._save_page(date_str, app_id, response.content)
                self.proxy_service.manage(proxy, False)
            else:
                raise
        except:
            self.proxy_service.manage(proxy, True)
            raise Exception('Failed scrape app page')

    def _save_page(self, date_str, app_id, content):
        output = open('res/{date}/{app_id}.txt'.format(date=date_str, app_id=app_id), 'w')
        output.write(content)
        output.close()
        print 'Succeed save app contents'

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
