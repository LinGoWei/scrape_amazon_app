# python src/scrape.py --after 1000

import argparse
import datetime
import requests
import random
import os

from bs4 import BeautifulSoup

from constant import user
from scrape_proxies import load_proxies
from utils import get_connection, retry

AMAZON_APP_URL = 'http://www.amazon.com/dp/{app_id}'
DIR_PATH = 'res/0512/{app_id}.txt'
APP_PAGE_SIZE = 10000
proxies = {"http://113.109.19.92:9999"}
error_proxies = dict()


def load_ids_set(db):
    cur = db.cursor()
    select_sql = "SELECT app_id FROM tb_products"
    cur.execute(select_sql)
    app_ids_set = set()
    for row in cur.fetchall():
        app_ids_set.add(row[0])
    return app_ids_set


def set_proxy():
    if not datetime.datetime.now().minute % 10:
        proxies.update(load_proxies('proxies.csv'))

    if not proxies:
        return None

    proxy = {
        'http': list(proxies)[random.randint(0, len(proxies))],
    }
    # print len(proxies), proxy
    return proxy


def scrape(app_ids_list):
    for app_id in app_ids_list:
        print 'Start', app_id
        if os.path.exists(DIR_PATH.format(app_id=app_id)):
            continue
        try:
            _scrape(app_id)
        except:
            print 'Failed scrape', app_id


@retry(3)
def _scrape(app_id):
    scrape_url = AMAZON_APP_URL.format(app_id=app_id)
    header = {'content-type': 'text/html', 'User-Agent': user[random.randint(0, len(user)-1)]}
    proxy = set_proxy()
    try:
        response = requests.get(scrape_url, timeout=30, headers=header, proxies=proxy)
        if len(response.content) > APP_PAGE_SIZE:
            print 'Succeed to scrape app page.'
            output = open('res/0512/{app_id}.txt'.format(app_id=app_id), 'w')
            output.write(response.content)
            output.close()
            print 'Succeed save app contents'
            manage_proxy(proxy, False)
        else:
            raise
    except:
        manage_proxy(proxy, True)
        print 'Failed'
        raise


def manage_proxy(proxy, error):
    if proxy:
        ip = proxy['http']
    else:
        return
    if error:
        # print 'Invalid', ip

        if ip in error_proxies:
            error_proxies[ip] += 1
            if error_proxies[ip] > 2:
                proxies.remove(ip)
                del error_proxies[ip]
                print 'Delete', proxy
        else:
            error_proxies[ip] = 1
    else:
        if ip in error_proxies:
            error_proxies[ip] -= 1


def parser(content):
    soup = BeautifulSoup(content, 'html.parser')
    name = soup.find(id='btAsinTitle').string
    description = soup.find(id='mas-product-description').div.contents[0]
    if not name or not description:
        print 'Failed to parser app name and description'
        return None
    return {'name': name, 'description': description}


def save(db, app_detail_dict):
    cur = db.cursor()
    for app_id, current_detail_dict in app_detail_dict.iteritems():
        last_detail_sql = "SELECT name, description FROM tb_products WHERE app_id='{}'".format(app_id)
        cur.execute(last_detail_sql)
        desc = cur.description
        last_detail_dict = {}
        for key, value in zip(desc, cur.fetchone()):
            last_detail_dict[key[0]] = value if value else 'NULL'
        if need_to_update(last_detail_dict, current_detail_dict):
            try:
                update_app(cur, app_id, current_detail_dict)
                print 'Succeed update app'
                save_event(cur, app_id, last_detail_dict, current_detail_dict)
                db.commit()
                print 'Succeed save event'
            except:
                db.rollback()


def need_to_update(last_detail_dict, current_detail_dict):
    if last_detail_dict['name'] != current_detail_dict['name']:
        return True


def save_event(cur, app_id, last_detail_dict, current_detail_dict):
    """Save event if found detail has been changed"""

    keys = ['app_id', 'old_name', 'old_desc', 'new_name', 'new_desc', 'date_time']
    val = [app_id, last_detail_dict['name'], last_detail_dict['description'], current_detail_dict['name'],
           current_detail_dict['description'], datetime.datetime.now().strftime('%Y-%m-%d')]
    insert_sql = """INSERT INTO tb_event ({}) VALUES ('{}')""".format(','.join(keys), "','".join(val))
    cur.execute(insert_sql)


def update_app(cur, app_id, current_detail_dict):
    values = list()
    for key, value in current_detail_dict.iteritems():
        values.append("{}='{}'".format(key, value))
        update_sql = """UPDATE tb_products SET {} WHERE app_id='{}'""".format(','.join(values), app_id)
        cur.execute(update_sql)


if __name__ == '__main__':
    par = argparse.ArgumentParser()
    par.add_argument(
        '--after', dest='after', type=int, required=True,
        help='Name of file '
    )
    args = par.parse_args()

    db = get_connection()
    app_ids_list = list(load_ids_set(db))
    scrape(app_ids_list[args.after:])

    app_detail_dict = dict()
    for app_id in app_ids_list:
        read_file = open('res/0512/{app_id}.txt'.format(app_id=app_id), 'r')
        content = read_file.read()
        detail = parser(content)
        if detail:
            app_detail_dict[app_id] = detail
    save(db, app_detail_dict)
    db.close()
    print 'Succeed to finish.'
