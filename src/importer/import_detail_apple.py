from bs4 import BeautifulSoup

from utils import get_logger
from import_detail_base import AppDetailImporter

__author__ = 'Blyde'


logger = get_logger(__name__)


class AppleDetailImporter(AppDetailImporter):
    def __init__(self):
        super(AppleDetailImporter, self).__init__()
        self.market = 'apple'

    def _parser(self, content):
        if not content:
            return None
        try:
            soup = BeautifulSoup(content, 'html.parser')
            name = soup.find(id='title').h1.string.encode('utf-8').replace('\"', '\'')
            description = soup.find(itemprop='description').get_text().encode('utf-8').replace('\"', '\'').replace('\\', '')
            icon_url = soup.find(id='left-stack').meta['content']
            if not name or not description or not icon_url:
                print 'Failed to parser app name, description and icon url.'
                logger.info('Failed to parser app name, description and icon url.')
                return None
            return {'name': name, 'description': description, 'icon_url': icon_url}
        except Exception as ex:
            logger.exception(ex)
