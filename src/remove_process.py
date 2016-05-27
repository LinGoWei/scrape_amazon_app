import argparse

from services.database_service import DatabaseService
from services.redis_service import RedisService
from constant import MARKET, DETAIL_SOURCE_KEY
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
    parse.add_argument(
        '--start', dest='start', type=int, required=True,
        help='Start id'
    )
    parse.add_argument(
        '--end', dest='end', type=int, required=True,
        help='End id'
    )
    args = parse.parse_args()
    
    redis_service = RedisService()
    app_ids = DatabaseService().load_ids(args.market, args.start, args.end)
    for app_id in app_ids:
        detail_key = DETAIL_SOURCE_KEY.format(market=args.market, date=args.date, app_id=app_id)
        print detail_key
        redis_service.delete(detail_key)
    
    print 'Succeed to finish.'
    logger.info('Succeed to finish.')
