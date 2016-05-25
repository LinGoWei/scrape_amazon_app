import random
import requests

from bs4 import BeautifulSoup

from constant import user_agents
from utils import retry, get_logger
from services.redis_service import RedisService

__author__ = 'Blyde'

PROXY_URL = '{protocol}://{ip}:{port}'
PROXY_URL_KEY = '{protocol}-proxy-urls'

DEFAULT_ERROR_TIMES = 1
logger = get_logger(__name__)


class ProxyService(object):
    def __init__(self):
        self.redis_service = RedisService()
        self.error_proxy_dict = dict()

    def get_proxy(self, protocol):
        proxy = self.redis_service.read_set(PROXY_URL_KEY.format(protocol=protocol))
        if not proxy:
            return None
        return {protocol: proxy}

    def get_valid_size(self, protocol):
        return self.redis_service.get_set_size(PROXY_URL_KEY.format(protocol=protocol))

    def process(self):
        logger.info('Start load proxy.')
        content = self._scrape_http_proxy()
        parser_proxy_url_set = self._parser_http_proxy(content)
        self._save('http', parser_proxy_url_set)

        content = self._scrape_https_proxy()
        parser_proxy_url_set = self._parser_https_proxy(content)
        self._save('https', parser_proxy_url_set)

    def manage(self, proxy, error):
        if not proxy:
            return
        protocol, proxy_url = proxy.items()[0]
        if error:
            if proxy_url in self.error_proxy_dict:
                self.error_proxy_dict[proxy_url] += 1
                if self.error_proxy_dict[proxy_url] > DEFAULT_ERROR_TIMES:
                    self.redis_service.pop_set(PROXY_URL_KEY.format(protocol=protocol), proxy_url)
                    self.error_proxy_dict.pop(proxy_url)
                    logger.info('Invalid proxy: {}'.format(proxy_url))
            else:
                self.error_proxy_dict[proxy_url] = 1
        else:
            if proxy_url in self.error_proxy_dict:
                self.error_proxy_dict[proxy_url] -= 1

    @retry(2)
    def _scrape_http_proxy(self):
        scrape_url = 'http://www.xicidaili.com/nn'
        header = {'content-type': 'text/html',
                  'User-Agent': user_agents[random.randint(0, len(user_agents)-1)]}
        try:
            response = requests.get(scrape_url, headers=header, proxies=None)
            return response.content
        except:
            raise Exception('Failed scrape proxies.')

    @retry(2)
    def _scrape_https_proxy(self):
        scrape_url = 'http://www.nianshao.me/?stype=2'
        header = {'content-type': 'text/html',
                  'User-Agent': user_agents[random.randint(0, len(user_agents)-1)]}
        try:
            response = requests.get(scrape_url, headers=header, proxies=None)
            return response.content
        except:
            raise Exception('Failed scrape proxies.')

    def _parser_http_proxy(self, content):
        soup = BeautifulSoup(content, 'html.parser')
        proxy_tag = soup.find(id='ip_list').select('tr')
        parser_proxy_url_set = set()
        for i in range(1, 21):
            proxy_url = PROXY_URL.format(protocol='http',
                                         ip=proxy_tag[i].find_all('td')[1].string,
                                         port=proxy_tag[i].find_all('td')[2].string)
            parser_proxy_url_set.add(proxy_url)
        return parser_proxy_url_set

    def _parser_https_proxy(self, content):
        soup = BeautifulSoup(content, 'html.parser')
        proxy_tag = soup.find_all('tr')
        parser_proxy_url_set = set()
        for i in range(1, 16):
            proxy_url = PROXY_URL.format(protocol='https',
                                         ip=proxy_tag[i].find_all('td')[0].string,
                                         port=proxy_tag[i].find_all('td')[1].string)
            parser_proxy_url_set.add(proxy_url)
        return parser_proxy_url_set

    def _save(self, protocol, parser_proxy_url_set):
        for url in parser_proxy_url_set:
            self.redis_service.add_set(PROXY_URL_KEY.format(protocol=protocol), url)
