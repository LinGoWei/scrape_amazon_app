
import redis
import requests
import random
import zlib
import string

from constant import user_agents, CATEGORY_PAGE_KEY
from services.redis_service import RedisService
from services.proxy_service import ProxyService
from utils import retry, get_logger

FREE_APPS_URL = 'http://www.apple.com/itunes/charts/free-apps/' 
PAID_APPS_URL = 'http://www.apple.com/itunes/charts/paid-apps/' 
SCRAPE_CATEGORY_URL = 'https://itunes.apple.com/genre/id{category_id}?letter={letter}&page={page}'

SCRAPE_PAGE_NUMBER = 10


logger = get_logger(__name__)

class AppleCategorySpider(object):
    def __init__(self):
        self.market = 'apple'
        self.request = requests.Session()
        self.redis_service = RedisService()
        self.proxy_service = ProxyService()

    def process(self, date_str, category_ids):
        print 'Start scrape {} on {}'.format(category_ids, date_str)
        logger.info('Start scrape {} on {}'.format(category_ids, date_str))
        for category_id in category_ids:
            for letter in string.uppercase:
                category_page_key = CATEGORY_PAGE_KEY.format(date=date_str, market=self.market, category_id=category_id, letter=letter)
                number = 0
                for page in range(1, SCRAPE_PAGE_NUMBER):
                    content = self._scrape(date_str, category_id, letter, page)
                    if content:
                        number += 1
                        self._save(category_page_key, content)

                print 'Succeed set {}, {}'.format(category_page_key, number)
                logger.info('Succeed set {}, {}'.format(category_page_key, number))

        logger.info('Finish scrape {} on {}'.format(category_ids, date_str))
        print 'Finish scrape {} on {}'.format(category_ids, date_str)

    def _scrape(self, date_str, category_id, letter, page):
        """Scrape category"""
        scrape_url = SCRAPE_CATEGORY_URL.format(category_id=category_id, letter=letter, page=page) 

        header = {'content-type': 'text/html',
                  'User-Agent': user_agents[random.randint(0, len(user_agents)-1)]}
        try:
            print 'Succeed scrape', scrape_url
            logger.info('Succeed scrape {}'.format(scrape_url))
            return self._scrape_category(scrape_url, header)
        except Exception as ex:
            logger.error(ex) 
            logger.error('Failed scrape {}'.format(scrape_url))
            print 'Failed scrape {}'.format(scrape_url)

    @retry(3)
    def _scrape_category(self, scrape_url, header):
        proxy = self.proxy_service.get_proxy('https')
        try:
            response = self.request.get(scrape_url, timeout=80, headers=header, proxies=proxy)
            if response:
                self.proxy_service.manage(proxy, False)
                return response.content
        except Exception as ex:
            self.proxy_service.manage(proxy, True)
            logger.exception(ex)
            raise ex

    def _save(self, category_page_key, content):
        try:
            self.redis_service.add_set(category_page_key, zlib.compress(content))
        except Exception as ex:
            logger.exception(ex)
            print 'Failed save category page {}'.format(category_page_key)
            logger.error('Failed save category page {}'.format(category_page_key))
