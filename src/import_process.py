import argparse

from constant import MARKET
from importer.import_amazon import AmazonDetailImporter
from importer.import_base import AppDetailImporter
from utils import get_logger

__author__ = 'Blyde'

logger = get_logger(__name__)


if __name__ == '__main__':
    parse = argparse.ArgumentParser()
    parse.add_argument(
        '--market', dest='market', choices=MARKET, required=True,
        help='Name of market'
    )
    parse.add_argument(
        '--date', dest='date', type=str, required=True,
        help='Date of detail need to import'
    )
    args = parse.parse_args()

    importer = AppDetailImporter()
    if args.market == 'amazon':
        importer = AmazonDetailImporter()
    elif args.market == 'apple':
        pass
    importer.imported(args.date)
    print 'Succeed to finish.'
    logger.info('Succeed to finish.')
