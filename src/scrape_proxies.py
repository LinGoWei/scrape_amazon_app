import csv
import random
from bs4 import BeautifulSoup
import datetime
import time
import requests

from constant import user

__author__ = 'Blyde'

PROXY_URL = 'http://{ip}:{port}'


def load_proxies(file_name):
    with file(file_name, 'r') as csv_file:
        reader = csv.reader(csv_file)
        proxies = list()
        for p in reader:
            proxies.extend(p)
        print 'Load proxies.'
        return set(proxies)


def scrape_proxies(file_name):
    content = scrape()
    proxies = parser(content)
    save(proxies, file_name)


def scrape():
    scrape_url = 'http://www.xicidaili.com/nn'
    print 'Start to scrape proxy.'
    header = {'content-type': 'text/html', 'User-Agent': user[random.randint(0, 820)]}
    response = requests.get(scrape_url, headers=header)
    return response.content


def parser(content):
    soup = BeautifulSoup(content, 'html.parser')
    proxy_tag = soup.find(id='ip_list').select('tr')
    proxies = list()
    for i in range(1, 20):
        proxies.append(PROXY_URL.format(ip=proxy_tag[i].find_all('td')[1].string, port=proxy_tag[i].find_all('td')[2].string))
    return proxies


def save(proxies, file_name):
    with open(file_name, 'w') as report_file:
        writer = csv.writer(report_file, delimiter=',', quotechar='"')
        for p in proxies:
            writer.writerow([p])
        print 'Succeed to save proxy.'


if __name__ == '__main__':
    while True:
        if not datetime.datetime.now().minute % 5:
            scrape_proxies('proxies.csv')
            time.sleep(40)
