# coding: utf-8

from gluish.intervals import (daily, weekly, biweekly, monthly, quarterly,
                              semiyearly, yearly)
import datetime
import unittest

class IntervalsTest(unittest.TestCase):

    def test_intervals(self):
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
