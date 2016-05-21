import random
import requests

from bs4 import BeautifulSoup

from constant import user_agents
from utils import retry, get_logger
from services.redis_service import RedisService

__author__ = 'Blyde'

PROXY_URL = '{protocol}://{ip}:{port}'
PROXY_URL_KEY = 'proxy-urls'

DEFAULT_ERROR_TIMES = 1
logger = get_logger(__name__)


class ProxyService(object):
    def __init__(self):
        self.redis_service = RedisService()
        self.error_proxy_dict = dict()

    def get_proxy(self):
        proxy = self.redis_service.read_set(PROXY_URL_KEY)
        if not proxy:
            return None
        return {'http': proxy}

    def get_valid_size(self):
        return self.redis_service.get_set_size(PROXY_URL_KEY)

    def process(self):
        logger.info('Start load proxy.')
        content = self._scrape()
        parser_proxy_url_set = self._parser(content)
        self._save(parser_proxy_url_set)

    def manage(self, proxy, error):
        if not proxy:
            return
        proxy_url = proxy['http']
        if error:
            if proxy_url in self.error_proxy_dict:
                self.error_proxy_dict[proxy_url] += 1
                if self.error_proxy_dict[proxy_url] > DEFAULT_ERROR_TIMES:
                    self.redis_service.pop_set(PROXY_URL_KEY, proxy_url)
                    self.error_proxy_dict.pop(proxy_url)
                    logger.info('Invalid proxy: {}'.format(proxy_url))
            else:
                self.error_proxy_dict[proxy_url] = 1
        else:
            if proxy_url in self.error_proxy_dict:
                self.error_proxy_dict[proxy_url] -= 1

    @retry(2)
    def _scrape(self):
        scrape_url = 'http://www.xicidaili.com/nn'
        header = {'content-type': 'text/html',
                  'User-Agent': user_agents[random.randint(0, len(user_agents)-1)]}
        try:
            response = requests.get(scrape_url, headers=header, proxies=None)
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
        return parser_proxy_url_set

    def _save(self, parser_proxy_url_set):
        for url in parser_proxy_url_set:
            self.redis_service.add_set(PROXY_URL_KEY, url)
