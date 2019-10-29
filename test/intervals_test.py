# coding: utf-8
# pylint: disable=C0103
#
#  Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                 by The Finc Authors, http://finc.info
#                 by Martin Czygan, <martin.czygan@uni-leipzig.de>
#
# This file is part of some open source application.
#
# Some open source application is free software: you can redistribute
# it and/or modify it under the terms of the GNU General Public
# License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.
#
# Some open source application is distributed in the hope that it will
# be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
#
# @license GPL-3.0+ <http://spdx.org/licenses/GPL-3.0+>
#

"""
Test intervals.
"""

import datetime
import unittest

import pytz
from dateutil.tz import tzlocal
from gluish.intervals import (biweekly, daily, every_minute, hourly, monthly,
                              quarterly, semiyearly, weekly, yearly)


class IntervalsTest(unittest.TestCase):
    """ Test intervals. """

    def test_short_intervals(self):
        dt = datetime.datetime(2000, 12, 3, 10, 10, 10, 0, pytz.utc)
        self.assertEqual('975834601', every_minute(dt, fmt='%s'))

        dt = datetime.datetime(2014, 6, 24, 13, 57, 59, 0, pytz.utc)
        self.assertEqual('1403611261', hourly(dt, fmt='%s'))

        dt = datetime.datetime(2014, 6, 24, 13, 1, 1, 0, pytz.utc)
        self.assertEqual('1403611261', hourly(dt, fmt='%s'))

        dt = datetime.datetime(2014, 6, 24, 12, 59, 59, 0, pytz.utc)
        self.assertEqual('1403607661', hourly(dt, fmt='%s'))

    test_short_intervals.notravis = 1

    def test_intervals(self):
        """ Basic intervals tests. """

        self.assertEqual(
            datetime.datetime(2000, 12, 3, 10, 10, 1, 0, pytz.utc),
            every_minute(datetime.datetime(2000, 12, 3, 10, 10, 10, 0, pytz.utc)))

        self.assertEqual(
            datetime.datetime(2000, 12, 3, 10, 1, 1, 0, pytz.utc),
            hourly(datetime.datetime(2000, 12, 3, 10, 10, 10, 0, pytz.utc)))

#
# Daily or less often
#
        self.assertEqual(
            datetime.date(2000, 12, 3),
            daily(datetime.date(2000, 12, 3)))

        self.assertEqual(
            # 2000-11-27 was a Monday
            datetime.date(2000, 11, 27),
            weekly(datetime.date(2000, 12, 3)))

        self.assertEqual(
            datetime.date(2000, 12, 1),
            biweekly(datetime.date(2000, 12, 12)))

        self.assertEqual(
            datetime.date(2000, 12, 15),
            biweekly(datetime.date(2000, 12, 16)))

        self.assertEqual(
            datetime.date(2000, 12, 1),
            monthly(datetime.date(2000, 12, 12)))

        self.assertEqual(
            datetime.date(2000, 12, 1),
            monthly(datetime.date(2000, 12, 12)))

        self.assertEqual(
            datetime.date(2000, 10, 1),
            quarterly(datetime.date(2000, 12, 12)))

        self.assertEqual(
            datetime.date(2000, 7, 1),
            semiyearly(datetime.date(2000, 12, 12)))

        self.assertEqual(
            datetime.date(2000, 1, 1),
            yearly(datetime.date(2000, 12, 12)))
