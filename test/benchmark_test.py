# coding: utf-8

"""
Benchmark tests.
"""

from gluish.benchmark import Timer
import unittest

class BenchmarkTest(unittest.TestCase):
    """ Benchmark tests. """

    def test_timer(self):
        """ Test timer timeframes are reasonable. """
        with Timer() as timer:
            pass
        self.assertTrue(0 < timer.elapsed_s < 1)
        self.assertTrue(0 < timer.elapsed_ms < 100)
