import time
import argparse
from multiprocessing import Process


from constant import MARKET
from services.database_service import DatabaseService
from scrape.scrape_detail_apple import multi_process_scrape_apple
from utils import get_logger
from scrape.scrape_detail_amazon import multi_process_scrape_amazon


__author__ = 'Blyde'

logger = get_logger(__name__)


class ScrapeProcess(object):
    def __init__(self, args):
        self.market = args.market
        self.after = args.after
        self.date = args.date
        self.batch = args.batch
        self.process_pool = []

    def process(self):
        ids = self._load_ids()[self.after:]
        self._create_process(ids)
        self._start_process()
        time.sleep(5)
        self._join_process()

    def _load_ids(self):
        data_service = DatabaseService()
        ids = data_service.load_ids(self.market)
        data_service.close()
        del data_service
        return ids

    def _create_process(self, ids):
        target = None
        if self.market == 'amazon':
            target = multi_process_scrape_amazon
        elif self.market == 'apple':
            target = multi_process_scrape_apple

        batch_size = len(ids) / self.batch + 1
        for process_id in range(0, self.batch):
            app_ids = ids[process_id * batch_size: (process_id+1) * batch_size]
            self.process_pool.append(Process(target=target, args=(process_id, self.date, app_ids)))

    def _start_process(self):
        for process in self.process_pool:
            process.start()

    def _join_process(self):
        for process in self.process_pool:
            process.join()


if __name__ == '__main__':
    parse = argparse.ArgumentParser()
    parse.add_argument(
        '--market', dest='market', choices=MARKET, required=True,
        help='Name of market'
    )
    parse.add_argument(
        '--after', dest='after', type=int, default=0,
        help='Id of after'
    )
    parse.add_argument(
        '--date', dest='date', type=str, required=True,
        help='Name of file'
    )
    parse.add_argument(
        '--batch', dest='batch', type=int, default=1,
        help='Batch of ids'
    )
    args = parse.parse_args()
    scrape_process = ScrapeProcess(args)
    scrape_process.process()
    print 'Succeed to finish.'
    logger.info('Succeed to finish')
