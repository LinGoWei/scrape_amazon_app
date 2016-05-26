import zlib
import gc
import string

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
            import_ids_set = set()
            for letter in string.uppercase:
                for content in self._load(date_str, category_id, letter):
                    import_ids_set.update(self._parser(content))
                
            print len(import_ids_set)
            self._save(import_ids_set)
            garbage_number = gc.collect()
            print 'Garbage number:', garbage_number
        
        self.database_service.close()

    def _load(self, date_str, category_id, letter):
        category_page_key = CATEGORY_PAGE_KEY.format(date=date_str, category_id=category_id, market=self.market, letter=letter)
        print category_page_key
        category_pages = self.redis_service.members_set(category_page_key)
        for category_page in category_pages:
            yield zlib.decompress(category_page)

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
            self.database_service.import_ids(self.market, ids_set)
            print 'Succeed import ids: {}'.format(len(ids_set))
            logger.info('Succeed import ids: {}'.format(len(ids_set)))

        except Exception as ex:
            logger.exception(ex)
            print ex
            logger.error('Failed import ids {}'.format(len(ids_set)))
            print 'Failed import ids.'
