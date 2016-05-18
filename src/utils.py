import logging
import MySQLdb

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
        yield l[i:i+size]


def get_connection():
    return MySQLdb.connect(host="localhost", user="lgw", passwd="", db="app_db")


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


def get_logger(log_file, name):
    file_handler = logging.FileHandler(log_file, mode="a", encoding="UTF-8")
    fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
    formatter = logging.Formatter(fmt)
    file_handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)
    return logger


def load_ids_set(date_base):
    cursor = date_base.cursor()
    select_sql = "SELECT app_id FROM tb_products"
    cursor.execute(select_sql)
    app_ids_set = set()
    for row in cursor.fetchall():
        app_ids_set.add(row[0])
    return app_ids_set
