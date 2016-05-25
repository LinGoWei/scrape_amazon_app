import argparse

from constant import MARKET, ACTION
from importer.import_detail_amazon import AmazonDetailImporter
from importer.import_detail_base import AppDetailImporter
from utils import get_logger
from importer.import_ids_base import AppIdsImporter
from importer.import_ids_apple import AppleIdsImporter
from importer.import_detail_apple import AppleDetailImporter

__author__ = 'Blyde'

logger = get_logger(__name__)


MARKET_TO_IMPORTER = {
    'amazon': {
        'detail': AmazonDetailImporter,
        'top_chart': AppIdsImporter,
    },
    'apple': {
        'detail': AppleDetailImporter,
        'top_chart': AppleIdsImporter,
    }
}


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
    parse.add_argument(
        '--action', dest='action', choices=ACTION, required=True,
        help='Name of action'
    )
    args = parse.parse_args()

    importer = MARKET_TO_IMPORTER[args.market][args.action] 
    importer().imported(args.date)
    print 'Succeed to finish.'
    logger.info('Succeed to finish.')
