# coding: utf-8

"""
Benchmark tests.
"""

from gluish.benchmark import ptimed
from gluish.benchmark import Timer
from gluish.database import sqlite3db
import datetime
import os
import tempfile
import unittest

BENCHMARK_DB = tempfile.mkstemp(prefix='gluish-')[1]

class Dummy(object):
    """ Dummy object, since timed and ptimed only work on methods so far. """
    def __init__(self, count=100000):
        self.count = count

    @ptimed(path=BENCHMARK_DB)
    def dummy_sum(self):
        """ Sum the first count numbers in a way Gauss would not do. """
        total = 0
        for i in xrange(self.count):
            total += i
        return total


class BenchmarkTest(unittest.TestCase):
    """ Benchmark tests. """

    def tearDown(self):
        """ Cleanup test db file, if it is still around. """
        if os.path.exists(BENCHMARK_DB):
            os.remove(BENCHMARK_DB)

    def test_timer(self):
        """ Test timer timeframes are reasonable. """
        with Timer() as timer:
            pass
        self.assertTrue(0 <= timer.elapsed_s <= 1)
        self.assertTrue(0 <= timer.elapsed_ms < 100)

    def test_ptimed(self):
        """ Test the persistent timed on a tempfile db. """
        self.assertTrue(os.path.exists(BENCHMARK_DB))
        dummy = Dummy()
        dummy.dummy_sum()
        with sqlite3db(BENCHMARK_DB) as cursor:
            cursor.execute("SELECT count(*) FROM t")
            self.assertEquals(1, cursor.fetchone()[0])
            cursor.execute("SELECT * FROM t")
            name, elapsed, added, status = cursor.fetchone()
            self.assertEquals(name, 'benchmark_test.Dummy.dummy_sum')
            self.assertTrue(0 < elapsed)
            self.assertTrue(1 > elapsed)
            self.assertEquals('green', status)
            self.assertTrue(added.startswith(str(datetime.date.today())))
