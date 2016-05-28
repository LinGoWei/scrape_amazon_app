from abc import abstractmethod
import requests
import zlib
import gc

from services.proxy_service import ProxyService
from services.redis_service import RedisService
from constant import DETAIL_SOURCE_KEY
from utils import get_logger, chunks

__author__ = 'Blyde'

logger = get_logger(__name__)
DEFAULT_BATCH_SIZE = 30


class AppDetailSpider(object):
    def __init__(self, error_dict):
        self.market = None
        self.proxy_service = ProxyService(error_dict)
        self.redis_service = RedisService()
        self.request = requests.Session()

    def process(self, process_id, date_str, app_ids):
        """

        :param date_str:
        :param app_ids:
        :return:
        """
        print 'Started process, need to scrape {}'.format(len(app_ids))
        logger.info('Started process, need to scrape {}'.format(len(app_ids)))
        for batch_app_ids in chunks(app_ids, DEFAULT_BATCH_SIZE):
            for app_id in batch_app_ids:
                app_detail_key = DETAIL_SOURCE_KEY.format(date=date_str, market=self.market, app_id=app_id)
                if self.redis_service.exists(app_detail_key):
                    continue
                content = self._scrape(app_id)
                if content:
                    self._save(app_detail_key, content)

            garbage_number = gc.collect()
            print 'Garbage number:', garbage_number

        print 'Succeed process {}'.format(process_id)
        logger.info('Succeed process {}'.format(process_id))

    def _scrape(self, app_id):
        try:
           return self._scrape_market(app_id)
        except Exception as ex:
            logger.exception(ex)
            print 'Failed scrape', app_id

    @abstractmethod
    def _scrape_market(self, app_id):
        """ Core scrape code in different market.

        :param app_id:
        :return: response.content
        """
        raise NotImplementedError()

    def _save(self, app_detail_key, content):
        try:
            self.redis_service.set(app_detail_key, zlib.compress(content))
            logger.info('Succeed set key: {}'.format(app_detail_key))
            print 'Succeed set key: {}'.format(app_detail_key)
        except Exception as ex:
            logger.exception(ex)
            logger.error('Failed set {}'.format(app_detail_key))
            print 'Failed set key', app_detail_key

