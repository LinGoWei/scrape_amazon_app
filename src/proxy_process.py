import time

from services.proxy_service import ProxyService


if __name__ == '__main__':
    proxy_service = ProxyService()
    while True:
        proxy_service.process()
        print 'Succeed scrape proxy.'
        time.sleep(20*60)
