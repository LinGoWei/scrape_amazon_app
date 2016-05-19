from abc import abstractmethod
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3 import Retry
import zlib
import time
import thread

from services.proxy_service import ProxyService
from services.redis_service import RedisService
from constant import DETAIL_SOURCE_KEY
from utils import get_logger

__author__ = 'Blyde'

logger = get_logger(__name__)


class AppDetailSpider(object):
    def __init__(self):
        self.proxy_service = ProxyService()
        self.redis_service = RedisService()
        self.request = requests.Session()
        retries = Retry(total=2, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
        self.request.mount('http://', HTTPAdapter(max_retries=retries))

    def process(self, date_str, platform, app_ids):
        """

        :param date_str:
        :param platform:
        :param app_ids:
        :return:
        """
        thread.start_new_thread(self._load_proxies_thread, (600, ))
        for app_detail_key, content in self._scrape(date_str, platform, app_ids):
            self._save(app_detail_key, content)

    def _scrape(self, date_str, platform, app_ids):
        for app_id in app_ids:
            app_detail_key = DETAIL_SOURCE_KEY.format(date=date_str, platform=platform, app_id=app_id)
            if self.redis_service.exists(app_detail_key):
                continue
            try:
                yield app_detail_key, self._scrape_market(app_id)
            except Exception as ex:
                logger.exception(ex)

    def _save(self, app_detail_key, content):
        try:
            self.redis_service.set(app_detail_key, zlib.compress(content))
            logger.info('Succeed set key: {}'.format(app_detail_key))
            print 'Succeed set key: {}'.format(app_detail_key)
        except Exception as ex:
            logger.exception(ex)
            logger.error('Failed set {}'.format(app_detail_key))
            print 'Failed set key', app_detail_key 

    @abstractmethod
    def _scrape_market(self, app_id):
        """ Core scrape code in different market.

        :param app_id:
        :return: response.content
        """
        raise NotImplementedError()

    def _load_proxies_thread(self, delay):
        logger.info('Start thread to load proxy.')
        while True:
            self.proxy_service.load_proxies('proxies.csv')
            size = self.proxy_service.get_valid_size()
            print 'Succeed load proxies. Valid size:', size
            logger.info('Succeed load proxies. Valid size {}'.format(size))
            time.sleep(delay)
