import time

from services.proxy_service import ProxyService


if __name__ == '__main__':
    proxy_service = ProxyService(None)
    while True:
        proxy_service.process()
        print 'Succeed scrape proxy.'
        time.sleep(3*60)
