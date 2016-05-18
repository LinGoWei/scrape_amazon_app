
import redis
import zlib
import re

from bs4 import BeautifulSoup

from constant import TOP_CHART_PAGE_KEY, CATEGORY_ID


PATTERN = "id[0-9]\d*"

class AppleTopChartParser(object):
    def __init__(self):
        self.reg = re.compile(PATTERN)

    def parser(self, content):
        ids = list()
        try:
            soup = BeautifulSoup(content, 'html.parser')
            a_tags = soup.find(id='selectedcontent').find_all('a')
            for a in a_tags:
                href = self.reg.search(a['href'])   
                if href:
                    ids.append(href.group(0))
            return ids
        except Exception as ex:
            print 'Falid parser.'


if __name__ == '__main__':

    redis_obj = redis.StrictRedis(host='localhost', port=6379, db=0)
    apple_top_chart_parser = AppleTopChartParser()

    for i in CATEGORY_ID:
        page_key = TOP_CHART_PAGE_KEY.format(date=date_str, category_id=i)
        content = zlib.decompress(redis_obj.get(page_key))
        ids = apple_top_chart_parser.parser(content) 
        print ids

