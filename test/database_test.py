# coding: utf-8

from gluish.database import sqlite3db
import unittest
import sqlite3
import tempfile


class DatabaseTest(unittest.TestCase):

    def test_sqlite3db(self):
        with sqlite3db(tempfile.mktemp()) as cursor:
            self.assertEquals(sqlite3.Cursor, cursor.__class__)
