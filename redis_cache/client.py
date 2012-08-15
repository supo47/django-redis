# -*- coding: utf-8 -*-

from __future__ import absolute_import

from django.core.exceptions import ImproperlyConfigured
from django.utils.encoding import smart_unicode, smart_str
from django.utils.datastructures import SortedDict
from django.utils import importlib

try:
    import cPickle as pickle
except ImportError:
    import pickle

from collections import defaultdict

import re

from redis import Redis
from redis.connection import DefaultParser
from redis import ConnectionPool
from redis.connection import UnixDomainSocketConnection, Connection
from redis.connection import DefaultParser

from .util import CacheKey, load_class
from .router import DummyRouter, MasterSlaveRouter
from .hash_ring import HashRing

class DefaultClient(object):
    def __init__(self, server, params, backend):
        self._pickle_version = -1
        self._backend = backend
        self._server = server
        self._params = params
        self._options = params.get('OPTIONS', {})

        self._connections = self.connect()
        self._serverdict = dict(self._connections)
        self._nodes = map(lambda x: x[0], self._connections)

        self.prepare_router()

        if "PICKLE_VERSION" in self._options:
            try:
                self._pickle_version = int(self._options['PICKLE_VERSION'])
            except (ValueError, TypeError):
                raise ImproperlyConfigured("PICKLE_VERSION value must be an integer")

    def prepare_router(self):
        if len(self._nodes) == 1:
            router_cls = DummyRouter
        else:
            router_cls = MasterSlaveRouter

        self._router = router_cls(self._serverdict, self._nodes)

    def connect(self):
        if not isinstance(self._server, (list, tuple)):
            self._server = [self._server]

        connections = []

        for connection_string in set(self._server):
            try:
                host, port, db = connection_string.split(":")
                port = int(port) if host != "unix" else port
                db = int(db)

            except (ValueError, TypeError):
                raise ImproperlyConfigured("Incorrect format '%s'" % (server))

            kwargs = {
                "db": db,
                "parser_class": self.parser_class,
                "password": self._options.get('PASSWORD', None),
            }

            if host == "unix":
                kwargs.update({'path': port, 'connection_class': UnixDomainSocketConnection})
            else:
                kwargs.update({'host': host, 'port': port, 'connection_class': Connection})

            connection_pool = ConnectionPool(**kwargs)
            connections.append((connection_string, Redis(connection_pool=connection_pool)))

        return connections

    @property
    def parser_class(self):
        cls = self._options.get('PARSER_CLASS', None)
        if cls is None:
            return DefaultParser

        return load_class(cls)

    def set(self, key, value, timeout=None, version=None, client=None):
        """
        Persist a value to the cache, and set an optional expiration time.
        """

        if not client:
            client = self._router.get_for_write()

        key = self.make_key(key, version=version)
        value = self.pickle(value)

        if timeout is None:
            timeout = self._backend.default_timeout

        if timeout > 0:
            return client.setex(key, value, int(timeout))
        return client.set(key, value)

    def incr_version(self, key, delta=1, version=None, client=None):
        """
        Adds delta to the cache version for the supplied key. Returns the
        new version.
        """

        if client is None:
            client = self._router.get_for_write()

        if version is None:
            version = self._backend.version

        old_key = self.make_key(key, version)
        value = self.get(old_key, version=version, client=client)
        ttl = client.ttl(old_key)

        if value is None:
            raise ValueError("Key '%s' not found" % key)

        if isinstance(key, CacheKey):
            new_key = self.make_key(key.original_key(), version=version+delta)
        else:
            new_key = self.make_key(key, version=version+delta)

        self.set(new_key, value, timeout=ttl)
        self.delete(old_key, client=client)
        return version + delta

    def add(self, key, value, timeout=None, version=None, client=None):
        """
        Add a value to the cache, failing if the key already exists.

        Returns ``True`` if the object was added, ``False`` if not.
        """

        if client is None:
            client = self._router.get_for_write()

        key = self.make_key(key, version=version)
        if client.exists(key):
            return False

        return self.set(key, value, timeout, client=client)

    def get(self, key, default=None, version=None, client=None):
        """
        Retrieve a value from the cache.

        Returns unpickled value if key is found, the default if not.
        """
        if client is None:
            client = self._router.get_for_read()

        key = self.make_key(key, version=version)
        value = client.get(key)

        if value is None:
            return default

        return self.unpickle(value)

    def delete(self, key, version=None, client=None):
        """
        Remove a key from the cache.
        """
        if client is None:
            client = self._router.get_for_write()

        client.delete(self.make_key(key, version=version))

    def delete_pattern(self, pattern, version=None, client=None):
        """
        Remove all keys matching pattern.
        """

        if client is None:
            client = self._router.get_for_write()

        pattern = self.make_key(pattern, version=version)
        keys = client.keys(pattern)
        client.delete(*keys)

    def delete_many(self, keys, version=None, client=None):
        """
        Remove multiple keys at once.
        """

        if client is None:
            client = self._router.get_for_write()

        if keys:
            keys = map(lambda key: self.make_key(key, version=version), keys)
            client.delete(*keys)

    def clear(self):
        """
        Flush all cache keys.
        """
        self._router.flushdb()

    def unpickle(self, value):
        """
        Unpickles the given value.
        """
        value = smart_str(value)
        return pickle.loads(value)

    def pickle(self, value):
        """
        Pickle the given value.
        """
        return pickle.dumps(value, self._pickle_version)

    def get_many(self, keys, version=None, client=None):
        """
        Retrieve many keys.
        """

        if client is None:
            client = self._router.get_for_read()

        if not keys:
            return {}

        recovered_data = SortedDict()

        new_keys = map(lambda key: self.make_key(key, version=version), keys)
        map_keys = dict(zip(new_keys, keys))

        results = client.mget(new_keys)
        for key, value in zip(new_keys, results):
            if value is None:
                continue
            recovered_data[map_keys[key]] = self.unpickle(value)
        return recovered_data

    def set_many(self, data, timeout=None, version=None, client=None):
        """
        Set a bunch of values in the cache at once from a dict of key/value
        pairs. This is much more efficient than calling set() multiple times.

        If timeout is given, that timeout will be used for the key; otherwise
        the default cache timeout will be used.
        """
        if client is None:
            client = self._router.get_for_write()

        pipeline = client.pipeline()
        for key, value in data.iteritems():
            self.set(key, value, timeout, version=version, client=pipeline)
        pipeline.execute()

    def incr(self, key, delta=1, version=None, client=None):
        """
        Add delta to value in the cache. If the key does not exist, raise a
        ValueError exception.
        """

        if client is None:
            client = self._router.get_for_write()

        key = self.make_key(key, version=version)

        if not client.exists(key):
            raise ValueError("Key '%s' not found" % key)

        value = self.get(key, version=version, client=client) + delta
        self.set(key, value, version=version, client=client)
        return value

    def decr(self, key, delta=1, version=None, client=None):
        """
        Decreace delta to value in the cache. If the key does not exist, raise a
        ValueError exception.
        """
        if client is None:
            client = self._router.get_for_write()

        key = self.make_key(key, version=version)

        if not client.exists(key):
            raise ValueError("Key '%s' not found" % key)

        value = self.get(key, version=version, client=client) - delta
        self.set(key, value, version=version, client=client)
        return value

    def has_key(self, key, version=None, client=None):
        """
        Test if key exists.
        """

        if client is None:
            client = self._router.get_for_read()

        key = self.make_key(key, version=version)
        return client.exists(key)

    # Other not default and not standar methods.
    def keys(self, search, client=None):
        if client is None:
            client =  self._router.get_for_read()

        pattern = self.make_key(search)
        return list(set(map(lambda x: x.split(":", 2)[2], client.keys(pattern))))

    def make_key(self, key, version=None):
        if not isinstance(key, CacheKey):
            key = CacheKey(self._backend.make_key(key, version))
        return key

    def close(self, **kwargs):
        self._router.close()


class ShardClient(DefaultClient):
    _findhash = re.compile('.*\{(.*)\}.*', re.I)

    def __init__(self, *args, **kwargs):
        SUper(ShardClient, self).__init__(*args, **kwargs)
        self._ring = HashRing(self._nodes)

    def get_server_name(self, _key):
        key = str(_key)
        g = _findhash.match(key)
        if g != None and len(g.groups()) > 0:
            key = g.groups()[0]
        name = self._ring.get_node(key)
        return name

    def get_server(self, key):
        name = self.get_server_name(key)
        return self._serverdict[name]

    def add(self,  key, value, timeout=None, version=None, client=None):
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super(ShardClient, self).add(key=key, value=value,
                                        version=version, client=client)

    def get(self,  key, default=None, version=None, client=None):
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)
        return super(ShardClient, self).get(key=key, default=default,
                                                version=version, client=client)

    def get_many(self, keys, version=None):
        if not keys:
            return {}

        recovered_data = SortedDict()
        new_keys = map(lambda key: self.make_key(key, version=version), keys)
        map_keys = dict(zip(new_keys, keys))

        for key in new_keys:
            client = self.get_server(key)
            value = self.get(key=key, version=version, client=client)

            if value is None:
                continue

            recovered_data[map_keys[key]] = value
        return recovered_data

    def set(self, key, value, timeout=None, version=None, client=None):
        """
        Persist a value to the cache, and set an optional expiration time.
        """
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super(ShardClient, self).set(key=key, value=value, timeout=timeout,
                                                        version=version, client=client)

    def set_many(self, data, timeout=None, version=None):
        """
        Set a bunch of values in the cache at once from a dict of key/value
        pairs. This is much more efficient than calling set() multiple times.

        If timeout is given, that timeout will be used for the key; otherwise
        the default cache timeout will be used.
        """
        for key, value in data.iteritems():
            self.set(key, value, timeout, version=version)

    def delete(self, key, version=None, client=None):
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super(ShardClient, self).delete(key=key, version=version, client=client)

    def delete_many(self, keys, version=None):
        """
        Remove multiple keys at once.
        """
        for key in map(lambda key: self.make_key(key, version=version), keys):
            client = self.get_server(key)
            self.delete(key, client=client)

    def incr_version(self, key, delta=1, version=None, client=None):
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super(ShardClient, self).incr_version(key=key, delta=delta,
                                                    version=version, client=client)

    def incr(self, key, delta=1, version=None, client=None):
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super(ShardClient, self).incr(key=key, delta=delta,
                                        version=version, client=client)

    def decr(self, key, delta=1, version=None, client=None):
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super(ShardClient, self).decr(key=key, delta=delta,
                                        version=version, client=client)

    #def delete_pattern(self, pattern, version=None, client=None):
    #    """
    #    Remove all keys matching pattern.
    #    """
    #    raise NotImplementedError
