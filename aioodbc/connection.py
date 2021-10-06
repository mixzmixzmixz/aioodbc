from typing import Awaitable, Callable, TypeVar, Optional
import asyncio
import sys
import traceback
import warnings
from functools import partial
from concurrent.futures import Executor

import pyodbc
from .cursor import Cursor
from .utils import _ContextManager, _is_conn_close_error


_T = TypeVar("_T")
__all__ = ['connect', 'Connection']


def connect(*,
    dsn: str | None,
    autocommit: bool = False,
    ansi: bool = False,
    timeout: int = 0,
    executor: Executor | None = None,
    echo = False,
    after_created: Optional[Callable] = None,
    **kwargs
):
    """Accepts an ODBC connection string and returns a new Connection object.

    The connection string can be passed as the string `str`, as a list of
    keywords,or a combination of the two.  Any keywords except autocommit,
    ansi, and timeout are simply added to the connection string.

    :param dsn: Dsn string

    :param autocommit: False or zero, the default, if True or non-zero,
        the connection is put into ODBC autocommit mode and statements are
        committed automatically.
    :param ansi: By default, pyodbc first attempts to connect using
        the Unicode version of SQLDriverConnectW. If the driver returns IM001
        indicating it does not support the Unicode version, the ANSI version
        is tried.
    :param timeout: An integer login timeout in seconds, used to set
        the SQL_ATTR_LOGIN_TIMEOUT attribute of the connection. The default is
         0  which means the database's default timeout, if any, is use
    :param after_created: support customize configuration after
        connection is connected.  Must be an async unary function, or leave it
        as None.
    :param executor: concurrent.futures.Executor to run on. If None - default is used
    """
    return _ContextManager(_connect(
        dsn=dsn, autocommit=autocommit, ansi=ansi, timeout=timeout, executor=executor,
        echo=echo, after_created=after_created, **kwargs
    ))


async def _connect(*,
    dsn: str | None,
    autocommit: bool = False,
    ansi: bool = False,
    timeout: int = 0,
    executor: Executor | None = None,
    echo = False,
    after_created: Optional[Callable] = None,
    **kwargs
):
    conn = Connection(
        dsn=dsn, autocommit=autocommit, ansi=ansi, timeout=timeout, echo=echo,
        executor=executor, after_created=after_created, **kwargs
    )
    await conn._connect()
    return conn


class Connection:
    """ Connection objects manage connections to the database.

    Connections should only be created by the aioodbc.connect function.
    """
    _source_traceback = None

    def __init__(
        self, *,
        dsn: str | None,
        autocommit: bool = False,
        ansi=None,
        timeout: int = 0,
        executor: Executor | None = None,
        echo: bool = False,
        after_created: Optional[Callable] = None,
        **kwargs
    ):
        self._executor = executor
        self._conn = None
        self._timeout = timeout
        self._last_usage = asyncio.get_running_loop().time()
        self._autocommit = autocommit
        self._ansi = ansi
        self._dsn = dsn
        self._echo = echo
        self._posthook = after_created
        self._kwargs = kwargs
        if asyncio.get_running_loop().get_debug():
            self._source_traceback = traceback.extract_stack(sys._getframe(1))

    def _execute(self, func: Callable[..., _T], *args, **kwargs) -> Awaitable[_T]:
        # execute function with args and kwargs in thread pool
        func = partial(func, *args, **kwargs)
        future = asyncio.get_running_loop().run_in_executor(self._executor, func)
        return future

    async def _connect(self) -> None:
        # create pyodbc connection
        f = self._execute(pyodbc.connect, self._dsn,
                          autocommit=self._autocommit, ansi=self._ansi,
                          timeout=self._timeout,
                          **self._kwargs)
        self._conn = await f
        if self._posthook is not None:
            await self._posthook(self._conn)

    @property
    def closed(self) -> bool:
        return self._conn is None

    @property
    def autocommit(self) -> bool:
        """Show autocommit mode for current database session. True if the
        connection is in autocommit mode; False otherwise. The default
        is False
        """
        return self._conn.autocommit

    @property
    def timeout(self) -> int:
        return self._conn.timeout

    @property
    def last_usage(self) -> float:
        return self._last_usage

    @property
    def echo(self) -> bool:
        return self._echo

    async def _cursor(self) -> Cursor:
        c = await self._execute(self._conn.cursor)
        self._last_usage = asyncio.get_running_loop().time()
        connection = self
        return Cursor(c, connection, echo=self._echo)

    def cursor(self) -> _ContextManager:
        return _ContextManager(self._cursor())

    async def close(self) -> Awaitable | None:
        """Close pyodbc connection"""
        if not self._conn:
            return
        c = await self._execute(self._conn.close)
        self._conn = None
        return c

    def commit(self) -> Awaitable:
        """Commit any pending transaction to the database."""
        fut = self._execute(self._conn.commit)
        return fut

    def rollback(self) -> Awaitable:
        """Causes the database to roll back to the start of any pending
        transaction.
        """
        fut = self._execute(self._conn.rollback)
        return fut

    async def execute(self, sql: str, *args) -> Cursor:
        """Create a new Cursor object, call its execute method, and return it.

        See Cursor.execute for more details.This is a convenience method
        that is not part of the DB API.  Since a new Cursor is allocated
        by each call, this should not be used if more than one SQL
        statement needs to be executed.

        :param sql: str, formatted sql statement
        :param args: tuple, arguments for construction of sql statement
        """
        try:
            _cursor = await self._execute(self._conn.execute, sql, *args)
            connection = self
            cursor = Cursor(_cursor, connection, echo=self._echo)
            return cursor
        except pyodbc.Error as e:
            if _is_conn_close_error(e):
                await self.close()
            raise

    def getinfo(self, type_: int) -> Awaitable:
        """Returns general information about the driver and data source
        associated with a connection by calling SQLGetInfo and returning its
        results. See Microsoft's SQLGetInfo documentation for the types of
        information available.

        :param type_: int, pyodbc.SQL_* constant
        """
        fut = self._execute(self._conn.getinfo, type_)
        return fut

    def add_output_converter(self, sqltype, func) -> Awaitable:
        """Register an output converter function that will be called whenever
        a value with the given SQL type is read from the database.

        :param sqltype: the integer SQL type value to convert, which can
            be one of the defined standard constants (pyodbc.SQL_VARCHAR)
            or a database-specific value (e.g. -151 for the SQL Server 2008
            geometry data type).
        :param func: the converter function which will be called with a
            single parameter, the value, and should return the converted
            value. If the value is NULL, the parameter will be None.
            Otherwise it will be a Python string.
        """
        fut = self._execute(self._conn.add_output_converter, sqltype, func)
        return fut

    def clear_output_converters(self) -> Awaitable:
        """Remove all output converter functions added by
        add_output_converter.
        """
        fut = self._execute(self._conn.clear_output_converters)
        return fut

    def set_attr(self, attr_id, value) -> Awaitable:
        """Calls SQLSetConnectAttr with the given values.

        :param attr_id: the attribute ID (integer) to set. These are ODBC or
            driver constants.
        :parm value: the connection attribute value to set. At this time
            only integer values are supported.
        """
        fut = self._execute(self._conn.set_attr, attr_id, value)
        return fut

    def __del__(self):
        if not self.closed:
            # This will block the loop, please use close
            # coroutine to close connection
            self._conn.close()
            self._conn = None

            warnings.warn("Unclosed connection {!r}".format(self),
                          ResourceWarning)

            context = {'connection': self,
                       'message': 'Unclosed connection'}
            if self._source_traceback is not None:
                context['source_traceback'] = self._source_traceback
            asyncio.get_running_loop().call_exception_handler(context)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return
