# coding: utf-8

"""
Test intervals.
"""

from gluish.intervals import (every_minute, hourly, daily, weekly, biweekly,
                              monthly, quarterly, semiyearly, yearly)
import datetime
import unittest

class IntervalsTest(unittest.TestCase):
    """ Test intervals. """
    def test_intervals(self):
        """ Basic intervals tests. """

        self.assertEquals(
            datetime.datetime(2000, 12, 3, 10, 10, 1),
            every_minute(datetime.datetime(2000, 12, 3, 10, 10, 10)))

        self.assertEquals(
            datetime.datetime(2000, 12, 3, 10, 1, 1),
            hourly(datetime.datetime(2000, 12, 3, 10, 10, 10)))

        self.assertEquals(
            datetime.date(2000, 12, 3),
            daily(datetime.date(2000, 12, 3)))

        self.assertEquals(
            # 2000-11-27 was a Monday
            datetime.date(2000, 11, 27),
            weekly(datetime.date(2000, 12, 3)))

        self.assertEquals(
            datetime.date(2000, 12, 1),
            biweekly(datetime.date(2000, 12, 12)))

        self.assertEquals(
            datetime.date(2000, 12, 15),
            biweekly(datetime.date(2000, 12, 16)))

        self.assertEquals(
            datetime.date(2000, 12, 1),
            monthly(datetime.date(2000, 12, 12)))

        self.assertEquals(
            datetime.date(2000, 12, 1),
            monthly(datetime.date(2000, 12, 12)))

        self.assertEquals(
            datetime.date(2000, 10, 1),
            quarterly(datetime.date(2000, 12, 12)))

        self.assertEquals(
            datetime.date(2000, 7, 1),
            semiyearly(datetime.date(2000, 12, 12)))

        self.assertEquals(
            datetime.date(2000, 1, 1),
            yearly(datetime.date(2000, 12, 12)))
