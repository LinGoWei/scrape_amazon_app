import redis
import zlib
import re

from bs4 import BeautifulSoup
from importer.import_ids_base import AppIdsImporter
from constant import APPLE_CATEGORY_ID

PATTERN = "id[0-9]\d*"

logger = get_logger(__name__)


class AppleIdsImporter(AppIdsImporter):
    def __init__(self):
        self.market = 'apple'
        self.category_ids = APPLE_CATEGORY_ID
        self.reg = re.compile(PATTERN)

    def parser(self, content):
        if not content:
            return None

        app_ids_set = set()
        try:
            soup = BeautifulSoup(content, 'html.parser')
            a_tags = soup.find(id='selectedcontent').find_all('a')
            for a in a_tags:
                href = self.reg.search(a['href'])
                if href:
                    app_ids_set.add(href.group(0))
            return app_ids_set
        except Exception as ex:
            logger.exception(ex)
            print 'Failed parser top chart.'
