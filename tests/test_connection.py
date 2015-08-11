import asyncio
import pyodbc
import unittest
import sys
import gc
from unittest import mock

from aioodbc.cursor import Cursor
from tests import base

from tests._testutils import run_until_complete


PY_341 = sys.version_info >= (3, 4, 1)


class TestConversion(base.ODBCTestCase):

    @run_until_complete
    def test_connect(self):
        conn = yield from self.connect()
        self.assertIs(conn.loop, self.loop)
        self.assertEqual(conn.autocommit, False)
        self.assertEqual(conn.timeout, 0)
        self.assertEqual(conn.closed, False)
        yield from conn.ensure_closed()

    @run_until_complete
    def test_basic_cursor(self):
        conn = yield from self.connect()
        cursor = yield from conn.cursor()
        sql = 'SELECT 10;'
        yield from cursor.execute(sql)
        (resp, ) = yield from cursor.fetchone()
        yield from conn.ensure_closed()
        self.assertEqual(resp, 10)

    @run_until_complete
    def test_default_event_loop(self):
        asyncio.set_event_loop(self.loop)

        conn = yield from self.connect(no_loop=True)
        cur = yield from conn.cursor()
        self.assertIsInstance(cur, Cursor)
        yield from cur.execute('SELECT 1;')
        (ret, ) = yield from cur.fetchone()
        self.assertEqual(1, ret)
        self.assertIs(conn._loop, self.loop)
        yield from conn.ensure_closed()

    @run_until_complete
    def test_close_twice(self):
        conn = yield from self.connect()
        yield from conn.ensure_closed()
        yield from conn.ensure_closed()
        self.assertTrue(conn.closed)

    @run_until_complete
    def test_execute(self):
        conn = yield from self.connect()
        cur = yield from conn.execute('SELECT 10;')
        (resp, ) = yield from cur.fetchone()
        yield from conn.ensure_closed()
        self.assertEqual(resp, 10)
        self.assertTrue(conn.closed)

    @run_until_complete
    def test_getinfo(self):
        conn = yield from self.connect()
        data = yield from conn.getinfo(pyodbc.SQL_CREATE_TABLE)
        self.assertEqual(data, 1793)
        yield from conn.ensure_closed()

    @run_until_complete
    def test_output_conversion(self):
        def convert(value):
            # `value` will be a string.  We'll simply add an X at the
            # beginning at the end.
            return 'X' + value + 'X'
        conn = yield from self.connect()
        yield from conn.add_output_converter(pyodbc.SQL_VARCHAR, convert)
        cur = yield from conn.cursor()

        yield from cur.execute("DROP TABLE IF EXISTS t1;")
        yield from cur.execute("CREATE TABLE t1(n INT, v VARCHAR(10))")
        yield from cur.execute("INSERT INTO t1 VALUES (1, '123.45')")
        yield from cur.execute("SELECT v FROM t1")
        (value, ) = yield from cur.fetchone()

        self.assertEqual(value, 'X123.45X')

        # Now clear the conversions and try again.  There should be
        # no Xs this time.
        yield from conn.clear_output_converters()
        yield from cur.execute("SELECT v FROM t1")
        (value, ) = yield from cur.fetchone()
        self.assertEqual(value, '123.45')
        yield from conn.ensure_closed()

    @run_until_complete
    def test_autocommit(self):
        conn = yield from self.connect(autocommit=True)
        self.assertEqual(conn.autocommit, True)
        yield from conn.ensure_closed()

    @run_until_complete
    def test_rollback(self):
        conn = yield from self.connect()
        self.assertEqual(conn.autocommit, False)

        cur = yield from conn.cursor()
        yield from cur.execute("DROP TABLE t1;")
        yield from cur.execute("CREATE TABLE t1(n INT, v VARCHAR(10));")

        yield from conn.commit()

        yield from cur.execute("INSERT INTO t1 VALUES (1, '123.45');")
        yield from cur.execute("SELECT v FROM t1")
        (value, ) = yield from cur.fetchone()
        self.assertEqual(value, '123.45')

        yield from conn.rollback()
        yield from cur.execute("SELECT v FROM t1;")
        value = yield from cur.fetchone()
        self.assertEqual(value, None)

        yield from conn.ensure_closed()

    @unittest.skipIf(not PY_341,
                     "Python 3.3 doesnt support __del__ calls from GC")
    @run_until_complete
    def test___del__(self):
        exc_handler = mock.Mock()
        self.loop.set_exception_handler(exc_handler)
        conn = yield from self.connect()
        with self.assertWarns(ResourceWarning):
            del conn
            gc.collect()

        msg = {'connection': mock.ANY,  # conn was deleted
               'message': 'Unclosed connection'}
        if self.loop.get_debug():
            msg['source_traceback'] = mock.ANY
        exc_handler.assert_called_with(self.loop, msg)
