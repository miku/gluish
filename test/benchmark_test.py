# coding: utf-8

from gluish.benchmark import Timer
import unittest

class BenchmarkTest(unittest.TestCase):

    def test_timer(self):
        with Timer() as timer:
            pass
        self.assertTrue(0 < timer.elapsed_s < 0.0001)
        self.assertTrue(0 < timer.elapsed_ms < 0.01)
