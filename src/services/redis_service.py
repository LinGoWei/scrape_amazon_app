__author__ = 'Blyde'

import redis


redis_info = {
    'host': "localhost",
    'port': 6379,
    'db': 0,
}


class RedisService(object):
    def __init__(self):
        self.redis_obj = redis.StrictRedis(host=redis_info['host'],
                                           port=redis_info['port'],
                                           db=redis_info['db'])

    def exists(self, key):
        return self.redis_obj.exists(key)

    def set(self, key, value):
        self.redis_obj.set(key, value)

    def get(self, key):
        return self.redis_obj.get(key)

    def push_list(self, key, values):
        self.redis_obj.delete(key)
        for val in values:
            self.redis_obj.lpush(key, val)

    def pull_list(self, key):
        return self.redis_obj.lrange(key, 0, -1)
