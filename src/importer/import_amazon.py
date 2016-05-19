from bs4 import BeautifulSoup

from utils import get_logger
from import_base import AppDetailImporter

__author__ = 'Blyde'


logger = get_logger(__name__)


class AmazonDetailImporter(AppDetailImporter):
    def __init__(self):
        super(AmazonDetailImporter, self).__init__()
        self.market = 'amazon'

    def _parser(self, content):
        soup = BeautifulSoup(content, 'html.parser')
        name = soup.find(id='btAsinTitle').string.encode('utf-8')
        description = soup.find(id='mas-product-description').div.contents[0].encode('utf-8').replace('\"', '')
        icon_url = soup.find(id='js-masrw-main-image')['src']
        print icon_url
        if not name or not description or not icon_url:
            print 'Failed to parser app name, description and icon url.'
            logger.info('Failed to parser app name, description and icon url.')
            return None
        return {'name': name, 'description': description, 'icon_url': icon_url}
