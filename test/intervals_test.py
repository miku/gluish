# coding: utf-8
# pylint: disable=C0103

"""
Test intervals.
"""

from gluish.intervals import (every_minute, hourly, daily, weekly, biweekly,
                              monthly, quarterly, semiyearly, yearly)
from dateutil.tz import tzlocal
import datetime
import pytz
import unittest

class IntervalsTest(unittest.TestCase):
    """ Test intervals. """
    def test_intervals(self):
        """ Basic intervals tests. """

        tz = tzlocal()

        dt = datetime.datetime(2000, 12, 3, 10, 10, 10)
        local_dt = dt.replace(tzinfo=tz)

        self.assertEquals('975831001',
            every_minute(local_dt.astimezone(pytz.utc), fmt='%s'))

        dt = datetime.datetime(2014, 6, 24, 13, 57, 59)
        local_dt = dt.replace(tzinfo=tz)

        self.assertEquals('1403600461',
            hourly(local_dt.astimezone(pytz.utc), fmt='%s'))

        dt = datetime.datetime(2014, 6, 24, 13, 1, 1)
        local_dt = dt.replace(tzinfo=tz)

        self.assertEquals('1403600461',
            hourly(local_dt.astimezone(pytz.utc), fmt='%s'))

        dt = datetime.datetime(2014, 6, 24, 12, 59, 59)
        local_dt = dt.replace(tzinfo=tz)

        self.assertEquals('1403596861',
            hourly(local_dt.astimezone(pytz.utc), fmt='%s'))

        self.assertEquals(
            datetime.datetime(2000, 12, 3, 10, 10, 1),
            every_minute(datetime.datetime(2000, 12, 3, 10, 10, 10)))

        self.assertEquals(
            datetime.datetime(2000, 12, 3, 10, 1, 1),
            hourly(datetime.datetime(2000, 12, 3, 10, 10, 10)))

#
# Daily or less often
#
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
