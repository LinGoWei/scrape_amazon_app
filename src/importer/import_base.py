from abc import abstractmethod
import zlib
import gc

from services.database_service import DatabaseService
from services.redis_service import RedisService
from utils import chunks
from utils import get_logger
from constant import DETAIL_SOURCE_KEY

__author__ = 'Blyde'

DEFAULT_BATCH_SIZE = 100

logger = get_logger(__name__)


class AppDetailImporter(object):
    def __init__(self):
        self.market = None
        self.database_service = DatabaseService()
        self.redis_service = RedisService()

    def imported(self, date_str):
        ids = self.database_service.load_ids(self.market)
        for batch_app_ids in chunks(ids, DEFAULT_BATCH_SIZE):
            print 'Started to import batch:', len(batch_app_ids)
            logger.info('Started to import batch: {}'.format(len(batch_app_ids)))
            for app_id in batch_app_ids:
                content = self._load(date_str, app_id)
                if not content:
                    continue
                detail_dict = self._parser(content)
                if not detail_dict:
                    continue
                self._save(app_id, detail_dict)

            garbage_number = gc.collect()
            print 'Garbage number:', garbage_number

        self.database_service.close()

    @abstractmethod
    def _parser(self, content):
        """

        :param content:
        :return: detail dict
        """
        raise NotImplementedError()

    def _save(self, app_id, current_detail):
        try:
            last_detail = self.database_service.get_app_detail(self.market, app_id)
            if self._need_to_update(last_detail, current_detail):
                self.database_service.update_app_detail(self.market, app_id, current_detail)
                self.database_service.save_event(app_id, last_detail, current_detail)
                logger.info('Succeed save detail and event for {} in {}'.format(app_id, self.market))
                print 'Succeed save detail and event for {} in {}'.format(app_id, self.market)
            else:
                logger.info('Not need to update for {} in {}'.format(app_id, self.market))
                print 'Not need to update for {} in {}'.format(app_id, self.market)

        except Exception as ex:
            logger.exception(ex)
            logger.info('Failed update detail and event for {} in {}'.format(app_id, self.market))
            print 'Failed save detail and event for {} in {}'.format(app_id, self.market)

    def _load(self, date_str, app_id):
        app_detail_key = DETAIL_SOURCE_KEY.format(date=date_str, market=self.market, app_id=app_id)
        # content = zlib.decompress(self.redis_service.get(app_detail_key))
        print app_detail_key
        content = self.redis_service.get(app_detail_key)
        return content

    @staticmethod
    def _need_to_update(last_detail_dict, current_detail_dict):
        if last_detail_dict['name'] != current_detail_dict['name']:
            return True
