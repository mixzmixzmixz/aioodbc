# copied from aiopg
# https://github.com/aio-libs/aiopg/blob/master/aiopg/pool.py

import asyncio
import collections

from .connection import connect
from .utils import _PoolContextManager, _PoolConnectionContextManager

__all__ = ['create_pool', 'Pool']


def create_pool(minsize: int = 10, maxsize: int = 10, echo: bool = False, pool_recycle: int = -1, **kwargs):
    return _PoolContextManager(
        _create_pool(minsize=minsize, maxsize=maxsize, echo=echo, pool_recycle=pool_recycle,**kwargs)
    )


async def _create_pool(
        minsize: int = 10, maxsize: int = 10, echo: bool = False, pool_recycle: int = -1,
        **kwargs
) -> 'Pool':
    pool = Pool(minsize=minsize, maxsize=maxsize, echo=echo, pool_recycle=pool_recycle, **kwargs)
    if minsize > 0:
        with (await pool._cond):
            await pool._fill_free_pool(False)
    return pool


class Pool(asyncio.AbstractServer):
    """Connection pool"""

    def __init__(self, minsize: int, maxsize: int, echo: bool, pool_recycle: int, **kwargs):
        if minsize < 0:
            raise ValueError("minsize should be zero or greater")
        if maxsize < minsize:
            raise ValueError("maxsize should be not less than minsize")
        self._minsize = minsize
        self._conn_kwargs = kwargs
        self._acquiring = 0
        self._recycle = pool_recycle
        self._free = collections.deque(maxlen=maxsize)
        self._cond = asyncio.Condition()
        self._used = set()
        self._closing = False
        self._closed = False
        self._echo = echo

    @property
    def echo(self) -> bool:
        return self._echo

    @property
    def minsize(self) -> int:
        return self._minsize

    @property
    def maxsize(self) -> int:
        return self._free.maxlen

    @property
    def size(self) -> int:
        return self.freesize + len(self._used) + self._acquiring

    @property
    def freesize(self) -> int:
        return len(self._free)

    @property
    def closed(self) -> bool:
        return self._closed

    async def clear(self) -> None:
        """Close all free connections in pool."""
        with (await self._cond):
            while self._free:
                conn = self._free.popleft()
                await conn.close()
            self._cond.notify()

    def close(self) -> None:
        """Close pool.

        Mark all pool connections to be closed on getting back to pool.
        Closed pool doesn't allow to acquire new connections.
        """
        if self._closed:
            return
        self._closing = True

    async def wait_closed(self) -> None:
        """Wait for closing all pool's connections."""

        if self._closed:
            return
        if not self._closing:
            raise RuntimeError(".wait_closed() should be called "
                               "after .close()")

        while self._free:
            conn = self._free.popleft()
            await conn.close()

        with (await self._cond):
            while self.size > self.freesize:
                await self._cond.wait()

        self._closed = True

    def acquire(self) -> _PoolConnectionContextManager:
        """Acquire free connection from the pool."""
        coro = self._acquire()
        return _PoolConnectionContextManager(coro, self)

    async def _acquire(self):
        if self._closing:
            raise RuntimeError("Cannot acquire connection after closing pool")
        with (await self._cond):
            while True:
                await self._fill_free_pool(True)
                if self._free:
                    conn = self._free.popleft()
                    assert not conn.closed, conn
                    assert conn not in self._used, (conn, self._used)
                    self._used.add(conn)
                    return conn
                else:
                    await self._cond.wait()

    async def _fill_free_pool(self, override_min):
        n, free = 0, len(self._free)
        while n < free:
            conn = self._free[-1]
            if -1 < self._recycle < asyncio.get_running_loop().time() - conn.last_usage:
                await conn.close()
                self._free.pop()
            else:
                self._free.rotate()
            n += 1

        while self.size < self.minsize:
            self._acquiring += 1
            try:
                conn = await connect(echo=self._echo, **self._conn_kwargs)
                # raise exception if pool is closing
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1
        if self._free:
            return

        if override_min and self.size < self.maxsize:
            self._acquiring += 1
            try:
                conn = await connect(echo=self._echo, **self._conn_kwargs)
                # raise exception if pool is closing
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1

    async def _wakeup(self):
        with (await self._cond):
            self._cond.notify()

    async def release(self, conn):
        """Release free connection back to the connection pool.
        """
        assert conn in self._used, (conn, self._used)
        self._used.remove(conn)
        if not conn.closed:
            if self._closing:
                await conn.close()
            else:
                self._free.append(conn)
            await self._wakeup()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()
        await self.wait_closed()
