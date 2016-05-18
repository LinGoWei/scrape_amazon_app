
import redis
import requests
import random
import zlib

from src.constant import user_agents, TOP_CHART_PAGE_KEY, CATEGORY_ID

FREE_APPS_URL = 'http://www.apple.com/itunes/charts/free-apps/' 
PAID_APPS_URL = 'http://www.apple.com/itunes/charts/paid-apps/' 
SCRAPE_CATEGORY_URL = 'https://itunes.apple.com/genre/id{category_id}'


class AppleTopChartSpider(object):
    def __init__(self):
        self.request = requests.Session()
        self.redis_obj = redis.StrictRedis(host='localhost', port=6379, db=0) 

    def scrape_top_chart(self):
        header = {'content-type': 'text/html',
                  'User-Agent': user_agents[random.randint(0, len(user_agents)-1)]}
        response = requests.get(FREE_APPS_URL, timeout=80, headers=header) 
        print len(response.content)

    def scrape_category(self, date_str, category_id):
        """Scrape category by id"""
        scrape_url = SCRAPE_CATEGORY_URL.format(category_id=category_id) 
        print scrape_url

        header = {'content-type': 'text/html',
                  'User-Agent': user_agents[random.randint(0, len(user_agents)-1)]}
        response = self.request.get(scrape_url, timeout=60, headers=header, verify=True) 
        page_key = TOP_CHART_PAGE_KEY.format(date=date_str, category_id=category_id)
        self._save_page(page_key, response.content)

    def _save_page(self, page_key, content):
        self.redis_obj.set(page_key, zlib.compress(content))


if __name__ == '__main__':
    apple_top_chart_spider = AppleTopChartSpider()
    for i in CATEGORY_ID:
        apple_top_chart_spider.scrape_category('0518', i)
