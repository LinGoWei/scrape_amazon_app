import random

from memory_profiler import profile
from urllib3 import ProxyManager, PoolManager

from constant import user_agents
from scrape.scrape_detail_base import AppDetailSpider
from utils import get_logger, retry

APPLE_APP_URL = 'https://itunes.apple.com/app/{app_id}'
REJECT_PAGE_SIZE = 10000    # 10K
NORMAL_APP_PAGE_SIZE = 50000   # 50K

logger = get_logger(__name__)


class AppleDetailSpider(AppDetailSpider):
    def __init__(self, error_dict):
        super(AppleDetailSpider, self).__init__(error_dict)
        self.market = 'apple'
        self.proxy = self.proxy_service.get_proxy('https')
        self.connection_pool = ProxyManager(self.proxy['https']) if self.proxy else PoolManager()

    @retry(2)
    def _scrape_market(self, app_id):
        scrape_url = APPLE_APP_URL.format(app_id=app_id)
        header = {'content-type': 'text/html',
                  'User-Agent': user_agents[random.randint(0, len(user_agents)-1)]}
        try:
            response = self.connection_pool.request('GET', scrape_url, timeout=60, retries=2, headers=header)
            if response:
                content = response.data
                if len(content) > REJECT_PAGE_SIZE:
                    if len(content) > NORMAL_APP_PAGE_SIZE:
                        self.proxy_service.manage(self.proxy, False)
                        print 'Succeed scrape app', app_id
                        logger.info('Succeed scrape app {}'.format(app_id))
                        return content
                    else:
                        print 'Invalid app', app_id
                        logger.info('Invalid app {}'.format(app_id))
                else:
                    logger.info('Reject visit app {}, use proxy {}'.format(app_id, self.proxy))
                    raise Exception('Reject visit app {}'.format(app_id))
            else:
                raise Exception('Response is None')

        except Exception as ex:
            self.proxy_service.manage(self.proxy, True)
            self.proxy = self.proxy_service.get_proxy('https')
            self.connection_pool = ProxyManager(self.proxy['https']) if self.proxy else PoolManager()
            raise ex

