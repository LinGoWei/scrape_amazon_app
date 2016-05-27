import random

from constant import user_agents
from scrape.scrape_detail_base import AppDetailSpider
from utils import get_logger, retry

APPLE_APP_URL = 'https://itunes.apple.com/app/{app_id}'
REJECT_PAGE_SIZE = 10000    # 10K
NORMAL_APP_PAGE_SIZE = 50000   # 50K

logger = get_logger(__name__)


class AppleDetailSpider(AppDetailSpider):
    def __init__(self):
        super(AppleDetailSpider, self).__init__()
        self.market = 'apple'

    @retry(3)
    def _scrape_market(self, app_id):
        scrape_url = APPLE_APP_URL.format(app_id=app_id)
        header = {'content-type': 'text/html',
                  'User-Agent': user_agents[random.randint(0, len(user_agents)-1)]}
        proxy = self.proxy_service.get_proxy('https')
        try:
            response = self.request.get(scrape_url, timeout=80, headers=header, proxies=proxy)
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
                logger.info('Reject visit app {}, use proxy {}'.format(app_id, proxy))
                raise Exception('Reject visit app {}'.format(app_id))

        except Exception as ex:
            self.proxy_service.manage(proxy, True)
            raise ex

