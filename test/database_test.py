# coding: utf-8

"""
Basic database tests.
"""

from gluish.database import sqlite3db
import os
import sqlite3
import sys
import tempfile
import unittest

class DatabaseTest(unittest.TestCase):
    """ Database tests. """

    def test_sqlite3db(self):
        """ Test CM yields correct object. """
        with sqlite3db(tempfile.mktemp()) as cursor:
            self.assertEquals(sqlite3.Cursor, cursor.__class__)

    def test_sqlite3db_copy_on_exit(self):
        """ Test copy_on_exit """
        if "sqlitebck" in sys.modules:
            target = tempfile.mktemp()
            with sqlite3db(":memory:", copy_on_exit=target) as cursor:
                cursor.execute("CREATE TABLE test (i INTEGER, t TEXT)")
                cursor.execute("INSERT INTO test VALUES (?, ?)",
                               (1, "Hello World"))

            self.assertTrue(os.path.exists(target))
            with sqlite3db(target) as cursor:
                cursor.execute("SELECT i, t FROM test LIMIT 1")
                row = cursor.fetchone()
                self.assertEquals((1, "Hello World"), row)
