import zlib
import gc

from constant import CATEGORY_PAGE_KEY
from utils import get_logger
from abc import abstractmethod
from utils import get_logger
from services.database_service import DatabaseService
from services.redis_service import RedisService

__author__ = 'Blyde'
logger = get_logger(__name__)

class AppIdsImporter(object):
    def __init__(self):
        self.market = None
        self.category_ids = None
        self.database_service = DatabaseService()
        self.redis_service = RedisService()

    def imported(self, date_str):
        print 'Started to import ids'
        logger.info('Started to import ids')
        for category_id in self.category_ids:
            content = self._load(date_str, category_id)
            ids_set = self._parser(content)
            self._save(ids_set)

        self.database_service.close()
        garbage_number = gc.collect()
        print 'Garbage number:', garbage_number

    def _load(self, date_str, category_id):
        # category_page_key = CATEGORY_PAGE_KEY.format(date=date_str, market=self.market, category_id=category_id)
        category_page_key = CATEGORY_PAGE_KEY.format(date=date_str, category_id=category_id)
        print category_page_key
        category_page = self.redis_service.get(category_page_key)
        if category_page:
            return zlib.decompress(category_page)

    @abstractmethod
    def _parser(self, content):
        """

        :param content:
        :return: ids set
        """
        raise NotImplementedError()

    def _save(self, ids_set):
        if not ids_set:
            return
        try:
            print 11
            self.database_service.import_ids(self.market, ids_set)
            print 'Succeed import ids: {}'.len(ids_set)
            logger.info('Succeed import ids: {}'.len(ids_set))

        except Exception as ex:
            logger.exception(ex)
            logger.error('Failed import ids.')
            print 'Failed import ids.'
