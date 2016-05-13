# python src/scrape.py --after 1000

import argparse
import requests
import random
import os

from bs4 import BeautifulSoup
import thread
import time

from constant import user_agents
from scrape_proxies import ProxyService
from utils import get_connection, retry

AMAZON_APP_URL = 'http://www.amazon.com/dp/{app_id}'
DIR_PATH = 'res/0512/{app_id}.txt'
APP_PAGE_SIZE = 10000
proxies = {"http://113.109.19.92:9999"}
error_proxies = dict()


def load_ids_set(date_base):
    cursor = date_base.cursor()
    select_sql = "SELECT app_id FROM tb_products"
    cursor.execute(select_sql)
    app_ids_set = set()
    for row in cursor.fetchall():
        app_ids_set.add(row[0])
    return app_ids_set


class AmazonAppSpider(object):
    def __init__(self):
        self.proxy_service = ProxyService()

    def scrape(self, date_str, app_ids_list):
        thread.start_new_thread(self._load_proxies, (600, ))

        for app_id in app_ids_list:
            print 'Started scrape', app_id
            if os.path.exists(DIR_PATH.format(app_id=app_id)):
                continue
            try:
                self._scrape(date_str, app_id)
            except:
                print 'Failed scrape', app_id

    @retry(2)
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


class AmazonAppParser(object):
    def parser(self, date_str, app_ids_list):
        app_detail_dict = dict()
        for app_id in app_ids_list:
            content = self._load_page(date_str, app_id)
            detail = self._parser(content)
            if detail:
                app_detail_dict[app_id] = detail
        return app_detail_dict

    def _load_page(self, date_str, app_id):
        read_file = open('res/{date}/{app_id}.txt'.format(date=date_str, app_id=app_id), 'r')
        content = read_file.read()
        return content

    def _parser(self, content):
        soup = BeautifulSoup(content, 'html.parser')
        name = soup.find(id='btAsinTitle').string
        description = soup.find(id='mas-product-description').div.contents[0]
        if not name or not description:
            print 'Failed to parser app name and description'
            return None
        return {'name': name, 'description': description}


class AmazonAppSaver(object):
    def __init__(self, date_base):
        self.date_base = date_base

    def save(self, app_detail_dict):
        cur = self.date_base.cursor()
        for app_id, current_detail_dict in app_detail_dict.iteritems():
            last_detail_sql = "SELECT name, description FROM tb_products WHERE app_id='{}'".format(app_id)
            cur.execute(last_detail_sql)
            desc = cur.description
            last_detail_dict = {}
            for key, value in zip(desc, cur.fetchone()):
                last_detail_dict[key[0]] = value if value else 'NULL'
            if self._need_to_update(last_detail_dict, current_detail_dict):
                try:
                    self._update_app(app_id, current_detail_dict)
                    print 'Succeed update app'
                    self._save_event(app_id, last_detail_dict, current_detail_dict)
                    db.commit()
                    print 'Succeed save event'
                except:
                    db.rollback()

    @staticmethod
    def _need_to_update(last_detail_dict, current_detail_dict):
        if last_detail_dict['name'] != current_detail_dict['name']:
            return True

    def _save_event(self, app_id, last_detail_dict, current_detail_dict):
        """Save event if found detail has been changed"""
        cursor = self.date_base.cursor()
        keys = ['app_id', 'old_name', 'old_desc', 'new_name', 'new_desc', 'date_time']
        val = [app_id, last_detail_dict['name'], last_detail_dict['description'], current_detail_dict['name'],
               current_detail_dict['description'], datetime.datetime.now().strftime('%Y-%m-%d')]
        insert_sql = """INSERT INTO tb_event ({}) VALUES ('{}')""".format(','.join(keys), "','".join(val))
        cursor.execute(insert_sql)

    def _update_app(self, app_id, current_detail_dict):
        cursor = self.date_base.cursor()
        values = list()
        for key, value in current_detail_dict.iteritems():
            values.append("{}='{}'".format(key, value))
            update_sql = """UPDATE tb_products SET {} WHERE app_id='{}'""".format(','.join(values), app_id)
            cursor.execute(update_sql)


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
    amazon_app_parser = AmazonAppParser()
    amazon_app_saver = AmazonAppSaver(db)

    amazon_app_spider.scrape(args.date, app_ids_list)
    # app_detail_dict = amazon_app_parser.parser(args.date, app_ids_list)
    # amazon_app_saver.save(app_detail_dict)
    db.close()
    print 'Succeed to finish.'
