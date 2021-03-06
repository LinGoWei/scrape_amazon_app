import random
import requests
import re
import urllib

from bs4 import BeautifulSoup
from urllib3 import PoolManager, ProxyManager

from constant import user_agents
from utils import retry, get_logger
from services.redis_service import RedisService

__author__ = 'Blyde'

PROXY_URL = '{protocol}://{ip}:{port}'
PROXY_URL_KEY = '{protocol}-proxy-urls'

CHECK_URL = {'http': 'http://www.amazon.com',
             'https': 'https://itunes.apple.com/app/id1046846443'}


DEFAULT_ERROR_TIMES = 2
logger = get_logger(__name__)


class ProxyService(object):
    def __init__(self, error_dict):
        self.redis_service = RedisService()
        self.error_proxy_dict = error_dict
        self.connection_pool = PoolManager()

    def get_proxy(self, protocol):
        proxy = self.redis_service.read_set(PROXY_URL_KEY.format(protocol=protocol))
        if not proxy:
            return None
        return {protocol: proxy}

    def get_valid_size(self, protocol):
        return self.redis_service.get_set_size(PROXY_URL_KEY.format(protocol=protocol))

    def process(self):
        logger.info('Start load proxy.')
        #content = self._scrape_http_proxy()
        #parser_proxy_url_set = self._parser_http_proxy(content)
        #self._save('http', self._check('http', parser_proxy_url_set))

        content = self._scrape_https_proxy()
        parser_proxy_url_set = self._parser_https_proxy(content)
        self._save('https', self._check('https', parser_proxy_url_set))

    def manage(self, proxy, error):
        if not proxy:
            return
        protocol, proxy_url = proxy.items()[0]
        if error:
            if proxy_url in self.error_proxy_dict:
                self.error_proxy_dict[proxy_url] += 1
                if self.error_proxy_dict[proxy_url] > DEFAULT_ERROR_TIMES:
                    self.redis_service.remove_set(PROXY_URL_KEY.format(protocol=protocol), proxy_url)
                    self.error_proxy_dict.pop(proxy_url)
                    logger.info('Invalid proxy: {}'.format(proxy_url))
                    print 'Invalid proxy'
            else:
                self.error_proxy_dict[proxy_url] = 1
        else:
            if proxy_url in self.error_proxy_dict:
                self.error_proxy_dict[proxy_url] -= 1
                if self.error_proxy_dict[proxy_url] < 1:
                    self.error_proxy_dict.pop(proxy_url)
        logger.info(self.error_proxy_dict)

    @retry(2)
    def _scrape_http_proxy(self):
        scrape_url = 'http://www.xicidaili.com/nn'
        header = {'content-type': 'text/html',
                  'User-Agent': user_agents[random.randint(0, len(user_agents)-1)]}
        try:
            response = self.request.get(scrape_url, headers=header, proxies=None)
            return response.content
        except:
            raise Exception('Failed scrape proxies.')

    @retry(2)
    def _scrape_https_proxy(self):
        #scrape_url = 'http://www.nianshao.me/?stype=2'
        scrape_url = 'http://proxy.moo.jp/zh/?c=&pt=&pr=HTTPS&a%5B%5D=0&a%5B%5D=1&a%5B%5D=2&u=60'
        header = {'content-type': 'text/html',
                  'Accept-Language': 'zh-CN,zh;q=0.8',
                  'User-Agent': user_agents[random.randint(0, len(user_agents)-1)]}
        response = self.connection_pool.request('GET', scrape_url, timeout=60, headers=header)
        return response.data

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
        proxy_tag = soup.find_all('tr', {'class': 'Odd'})
        res = re.compile('%(%|\w)+')
        parser_proxy_url_set = set()
        for i in range(0, 25):
            tds = proxy_tag[i].find_all('td')
            if not tds[0].string:
                continue
            ip_res = res.search(tds[0].string)
            if ip_res:
                ip = urllib.unquote(ip_res.group(0))
                port = tds[1].string
                proxy_url = PROXY_URL.format(protocol='https', ip=ip, port=port)
                parser_proxy_url_set.add(proxy_url)
        return parser_proxy_url_set

    def _check(self, protocol, proxy_url_set):
        valid_proxy_url_set = set()
        for url in proxy_url_set:
            header = {'content-type': 'text/html',
                      'User-Agent': user_agents[random.randint(0, len(user_agents)-1)]}
            proxy = {protocol: url}
            conection_pool = ProxyManager(url)
            try:
                response = conection_pool.request('GET', CHECK_URL[protocol], timeout=60, headers=header)
                if response.status == 200:
                    valid_proxy_url_set.add(url)
                    print 'Valid proxy url', url
                else:
                    print 'Invalid ', url
            except Exception as ex:
                print ex
                print 'Invalid ', url

        return valid_proxy_url_set

    def _save(self, protocol, parser_proxy_url_set):
        for url in parser_proxy_url_set:
            self.redis_service.add_set(PROXY_URL_KEY.format(protocol=protocol), url)
