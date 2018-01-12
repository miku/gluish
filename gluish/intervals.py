# coding: utf-8
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
Various intervals.
"""

import datetime

__all__ = [
    'biweekly',
    'daily',
    'every_minute',
    'hourly',
    'monthly',
    'quarterly',
    'semiyearly',
    'weekly',
    'yearly',
]

def every_minute(dt=datetime.datetime.utcnow(), fmt=None):
    """
    Just pass on the given date.
    """
    date = datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, 1, 0, dt.tzinfo)
    if fmt is not None:
        return date.strftime(fmt)
    return date

def hourly(dt=datetime.datetime.utcnow(), fmt=None):
    """
    Get a new datetime object every hour.
    """
    date = datetime.datetime(dt.year, dt.month, dt.day, dt.hour, 1, 1, 0, dt.tzinfo)
    if fmt is not None:
        return date.strftime(fmt)
    return date

def daily(date=datetime.date.today()):
    """
    Just pass on the given date.
    """
    return date

def weekly(date=datetime.date.today()):
    """
    Weeks start are fixes at Monday for now.
    """
    return date - datetime.timedelta(days=date.weekday())

def biweekly(date=datetime.date.today()):
    """
    Every two weeks.
    """
    return datetime.date(date.year, date.month, 1 if date.day < 15 else 15)

def monthly(date=datetime.date.today()):
    """
    Take a date object and return the first day of the month.
    """
    return datetime.date(date.year, date.month, 1)

def quarterly(date=datetime.date.today()):
    """
    Fixed at: 1/1, 4/1, 7/1, 10/1.
    """
    return datetime.date(date.year, ((date.month - 1)//3) * 3 + 1, 1)

def semiyearly(date=datetime.date.today()):
    """
    Twice a year.
    """
    return datetime.date(date.year, 1 if date.month < 7 else 7, 1)

def yearly(date=datetime.date.today()):
    """
    Once a year.
    """
    return datetime.date(date.year, 1, 1)
