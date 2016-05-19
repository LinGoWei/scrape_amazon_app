import random

from constant import user_agents
from scrape.scrape_detail_base import AppDetailSpider
from utils import get_logger, retry

AMAZON_APP_URL = 'http://www.amazon.com/dp/{app_id}'
REJECT_PAGE_SIZE = 10000    # 10K
NORMAL_APP_PAGE_SIZE = 100000   # 100K

logger = get_logger(__name__)


class AmazonAppSpider(AppDetailSpider):
    def __init__(self):
        super(AmazonAppSpider, self).__init__()
        self.market = 'amazon'

    @retry(2)
    def _scrape_market(self, app_id):
        scrape_url = AMAZON_APP_URL.format(app_id=app_id)
        header = {'content-type': 'text/html',
                  'User-Agent': user_agents[random.randint(0, len(user_agents)-1)]}
        proxy = self.proxy_service.get_proxy()
        try:
            response = self.request.get(scrape_url, timeout=60, headers=header, proxies=proxy)
            if len(response.content) > REJECT_PAGE_SIZE:
                if len(response.content) > NORMAL_APP_PAGE_SIZE:
                    self.proxy_service.manage(proxy, False)
                    print 'Succeed scrape app', app_id
                    logger.info('Succeed scrape app {}'.format(app_id))
                    return response.content
                else:
                    print 'Invalid app', app_id
                    logger.info('Invalid app {}'.format(app_id))
            else:
                raise Exception('Reject visit app')

        except Exception as ex:
            self.proxy_service.manage(proxy, True)
            logger.error(ex)
            print ex


def multi_process_scrape_amazon(process_id, date, ids):
    """" Multi process scrape amazon app"""
    print 'Start process {}, need to scrape {} apps in amazon'.format(process_id, len(ids))
    logger.info('Start process {}, need to scrape {} apps in amazon'.format(process_id, len(ids)))
    amazon_app_spider = AmazonAppSpider()
    amazon_app_spider.process(date, ids)
    print 'Succeed finish process', process_id
    logger.info('Succeed finish process {}'.format(process_id))
