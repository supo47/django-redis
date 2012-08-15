# -*- coding: utf-8 -*-

from django.core.cache.backends.base import BaseCache
from django.core.exceptions import ImproperlyConfigured
from django.core.cache import get_cache

from redis.exceptions import ConnectionError
from .util import load_class

import functools

REDIS_CACHE_FALLBACK_MAX_RETRY = 20

def _call_fallback_method(client, methodname, *args, **kwargs):
    try:
        return getattr(client, methodname)(*args, **kwargs)
    except AttributeError:
        # This is raised if you are execute non standar methods
        # available on RedisCache
        return None


def auto_fallback(method, backend):
    @functools.wraps(method)
    def _wrapper(*args, **kwargs):
        if backend._on_fallback:
            if backend._fallback_counter < REDIS_CACHE_FALLBACK_MAX_RETRY:
                backend._fallback_counter += 1
                return _call_fallback_method(backend.fallback_client, method.__name__, *args, **kwargs)

            backend._on_fallback = False

        try:
            return method(*args, **kwargs)
        except ConnectionError:
            backend._on_fallback = True
            backend._fallback_counter = 0
            return _call_fallback_method(backend.fallback_client, method.__name__, *args, **kwargs)

    return _wrapper


class RedisCache(BaseCache):
    def __init__(self, server, params):
        super(RedisCache, self).__init__(params)
        self._server = server
        self._params = params

        options = params.get('OPTIONS', {})
        self._client_cls = options.get('CLIENT_CLASS', 'redis_cache.client.DefaultClient')
        self._client_cls = load_class(self._client_cls)
        self._client = None

        self._fallback_name = options.get('FALLBACK', None)
        self._fallback = None
        self._fallback_counter = 0
        self._on_fallback = False

    @property
    def client(self):
        if self._client is None:
            self._client = self._client_cls(self._server, self._params, self)
        return self._client

    @property
    def fallback_client(self):
        if self._fallback is None:
            try:
                self._fallback = get_cache(self._fallback_name)
            except TypeError:
                raise ImproperlyConfigured("%s cache backend is not configured" % (self._fallback_name))
        return self._fallback

    def __getattribute__(self, name):
        """
        Intercept some methods and decorate these with
        auto_fallback decorator.
        """

        klass = object.__getattribute__(self, "__class__")
        if name in klass.__dict__ and name not in ["client", "fallback_client"]:
            return auto_fallback(object.__getattribute__(self, name), self)

        return object.__getattribute__(self, name)

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
