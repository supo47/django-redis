# -*- coding: utf-8 -*-

from django.core.cache.backends.base import BaseCache
from .util import load_class


class RedisCache(BaseCache):
    def __init__(self, server, params):
        super(RedisCache, self).__init__(params)
        self._server = server
        self._params = params

        options = params.get('OPTIONS', {})
        self._client_cls = options.get('CLIENT_CLASS', 'redis_cache.client.DefaultClient')
        self._client_cls = load_class(self._client_cls)
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = self._client_cls(self._server, self._params, self)
        return self._client

    #def __getattribute__(self, name):
    #    print "*"*20, name
    #    if name in ["close", "set", "get"]:
    #        return object.__getattribute__(self.client, name)
    #    return object.__getattribute__(self, name)

    def set(self, *args, **kwargs):
        return self.client.set(*args, **kwargs)

    def incr_version(self, *args, **kwargs):
        return self.client.incr_version(*args, **kwargs)

    def add(self, *args, **kwargs):
        return self.client.add(*args, **kwargs)

    def get(self, *args, **kwargs):
        return self.client.get(*args, **kwargs)

    def delete(self, *args, **kwargs):
        return self.client.delete(*args, **kwargs)

    def delete_pattern(self, *args, **kwargs):
        return self.client.delete_pattern(*args, **kwargs)

    def delete_many(self, *args, **kwargs):
        return self.client.delete_many(*args, **kwargs)

    def clear(self):
        return self.client.clear()

    def get_many(self, *args, **kwargs):
        return self.client.get_many(*args, **kwargs)

    def set_many(self, *args, **kwargs):
        return self.client.set_many(*args, **kwargs)

    def incr(self, *args, **kwargs):
        return self.client.incr(*args, **kwargs)

    def decr(self, *args, **kwargs):
        return self.client.decr(*args, **kwargs)

    def has_key(self, *args, **kwargs):
        return self.client.has_key(*args, **kwargs)

    def keys(self, *args, **kwargs):
        return self.client.keys(*args, **kwargs)

    def close(self, **kwargs):
        self.client.close(**kwargs)
