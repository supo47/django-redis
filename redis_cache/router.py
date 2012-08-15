# -*- coding: utf-8 -*-

import random


class BaseRouter(object):
    """
    Base class for all routers.
    """

    def __init__(self, servers, keys):
        self._servers = servers
        self._keys = keys

    def close(self):
        """
        Close all connections.
        """

        for key, server in self._servers.items():
            for c in server.connection_pool._available_connections:
                c.close()



class DummyRouter(BaseRouter):
    """
    Dummy router. Used with one unique connection and
    always return the same connection.
    """

    def __init__(self, servers, keys):
        super(DummyRouter, self).__init__(servers, keys)
        self.default_server = self._servers[keys[0]]

    def get_for_write(self):
        return self.default_server

    def get_for_read(self):
        return self.default_server

    def flushdb(self):
        return self.default_server.flushdb()


class MasterSlaveRouter(BaseRouter):
    """
    Default router for master-slave connections.
    """

    def __init__(self, servers, keys):
        super(MasterSlaveRouter, self).__init__(servers, keys)
        self.default_server = self._servers[keys[0]]

    def get_for_write(self):
        return self.default_server

    def get_for_read(self):
        random_key = random.choice(self._keys[1:])
        return self._servers[random_key]

    def flushdb(self):
        return self.default_server.flushdb()
