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

    def add_set(self, key, value):
        self.redis_obj.sadd(key, value)

    def read_set(self, key):
        return self.redis_obj.srandmember(key)

    def pop_set(self, key):
        return self.redis_obj.spop(key)

    def remove_set(self, key, value):
        self.redis_obj.srem(key, value)
    
    def members_set(self, key):
        return self.redis_obj.smembers(key)

    def get_set_size(self, key):
        return self.redis_obj.scard(key)
