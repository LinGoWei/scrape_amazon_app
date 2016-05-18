import logging
import datetime

__author__ = 'Blyde'


def chunks(l, size):
    """
    Yield successive n-sized chunks from l.
    :param list l: List to split in chunks
    :param int size: size of the chunks
    :return: generator which returns every chunk of the list
    :rtype: list
    """
    for i in xrange(0, len(l), size):
        yield l[i: i+size]


def retry(count):
    """
    This wrapper is used for retry the function(f) if has error,
    will continue for count times
    """
    def _f(f):
        def _retry(*args, **kwargs):
            for _ in range(count):
                try:
                    ret = f(*args, **kwargs)
                    return ret
                except Exception, e:
                    _e = e
            raise _e
        return _retry
    return _f


def get_logger(name):
    """Get a handler of logger"""
    log_file_name = 'app_scrape_{date}.log'.format(date=datetime.datetime.now().strftime('%Y-%m-%d'))
    file_handler = logging.FileHandler(log_file_name, mode="a", encoding="UTF-8")
    fmt = '%(asctime)s - %(process)d - %(thread)d - %(filename)s:%(lineno)s - %(name)s - %(message)s'
    formatter = logging.Formatter(fmt)
    file_handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)
    return logger
