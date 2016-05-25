import time
import argparse
from multiprocessing import Process

from constant import MARKET, APPLE_CATEGORY_IDS
from utils import get_logger
from scrape.scrape_apple_ids import AppleCategorySpider

__author__ = 'Blyde'

logger = get_logger(__name__)


class CategoryScrapeProcess(object):
    def __init__(self, args):
        self.market = args.market
        self.date = args.date
        self.batch = args.batch
        self.process_pool = []
        self.apple_category_spider = AppleCategorySpider()

    def process(self):
        self._create_process(APPLE_CATEGORY_IDS)
        self._start_process()
        time.sleep(2)
        self._join_process()

    def _create_process(self, ids):
        batch_size = len(ids)/ self.batch + 1
        for process_id in range(0, self.batch):
            category_ids = list(ids)[process_id * batch_size: (process_id+1) * batch_size]
            self.process_pool.append(Process(target=self.apple_category_spider.process, args=(self.date, category_ids)))

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
    category_scrape_process = CategoryScrapeProcess(args)
    category_scrape_process.process()
    print 'Succeed to finish.'
    logger.info('Succeed to finish')
