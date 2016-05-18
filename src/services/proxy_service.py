import csv
import random
import requests
import time

from bs4 import BeautifulSoup

from src.constant import user_agents
from src.utils import retry

__author__ = 'Blyde'

PROXY_URL = '{protocol}://{ip}:{port}'
BACKUP_PROXY_URL_SET = {
    'http://182.120.31.119:9999',
    'http://183.62.206.210:3128',
    'http://60.191.165.100:3128',
    'http://106.7.154.67:9000',
}
DEFAULT_ERROR_TIMES = 1


class ProxyService(object):
    def __init__(self):
        self.proxy_url_set = BACKUP_PROXY_URL_SET
        self.error_proxy_dict = dict()
        self.invalid_proxy_url_set = set()

    def get_proxy(self):
        proxy = list(self.proxy_url_set)[random.randint(0, len(self.proxy_url_set) - 1)]
        if not proxy:
            return None
        return {'http': proxy}

    def get_valid_size(self):
        return len(self.proxy_url_set)

    def load_proxies(self, file_name):
        with file(file_name, 'r') as csv_file:
            reader = csv.reader(csv_file)
            for p in reader:
                if p[0] not in self.invalid_proxy_url_set:
                    self.proxy_url_set.add(p[0])
                    # logger.info('Succeed load proxies.')

    def scrape_proxies(self, file_name):
        # logger.info('Started to scrape proxies.')
        content = self._scrape()
        parser_proxy_url_set = self._parser(content)
        self._save(file_name, parser_proxy_url_set)

    def manage(self, proxy, error):
        if not proxy:
            return
        proxy_url = proxy['http']
        if error:
            if proxy_url in self.error_proxy_dict:
                self.error_proxy_dict[proxy_url] += 1
                if self.error_proxy_dict[proxy_url] > DEFAULT_ERROR_TIMES:
                    self.proxy_url_set.remove(proxy_url)
                    self.error_proxy_dict.pop(proxy_url)
            else:
                self.error_proxy_dict[proxy_url] = 1
        else:
            if proxy_url in self.error_proxy_dict:
                self.error_proxy_dict[proxy_url] -= 1

    @retry(2)
    def _scrape(self):
        scrape_url = 'http://www.xicidaili.com/nn'
        header = {'content-type': 'text/html',
                  'User-Agent': user_agents[random.randint(0, len(user_agents) - 1)]}
        try:
            response = requests.get(scrape_url, headers=header, proxies=None)
            # logger.info('Succeed get proxies.')
            return response.content
        except:
            raise Exception('Failed scrape proxies.')

    @retry(2)
    def _parser(self, content):
        soup = BeautifulSoup(content, 'html.parser')
        proxy_tag = soup.find(id='ip_list').select('tr')
        parser_proxy_url_set = set()
        for i in range(1, 21):
            proxy_url = PROXY_URL.format(protocol='http',
                                         ip=proxy_tag[i].find_all('td')[1].string,
                                         port=proxy_tag[i].find_all('td')[2].string)
            parser_proxy_url_set.add(proxy_url)
        # logger.info('Succeed parser proxies')
        return parser_proxy_url_set

    def _save(self, file_name, parser_proxy_url_set):
        with open(file_name, 'w') as report_file:
            writer = csv.writer(report_file, delimiter=',', quotechar='"')
            for p in parser_proxy_url_set:
                writer.writerow([p])
                # logger.info('Succeed save proxies')


if __name__ == '__main__':
    proxy_service = ProxyService()
    while True:
        proxy_service.scrape_proxies('proxies.csv')
        print 'Succeed scrape proxies.'
        time.sleep(60 * 10)
