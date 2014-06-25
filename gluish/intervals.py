# coding: utf-8

"""
Intervals for luigi.DateParameter.

Usage:

    # run this on the 1st of every month ...
    date = luigi.DateParameter(date=intervals.monthly())

"""

import datetime

def every_minute(dt=datetime.datetime.now()):
    """ just pass on the given date """
    return datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, 1)

def hourly(dt=datetime.datetime.now()):
    """ get a new datetime object every hour """
    return datetime.datetime(dt.year, dt.month, dt.day, dt.hour, 1, 1)

def daily(date=datetime.date.today()):
    """ just pass on the given date """
    return date

def weekly(date=datetime.date.today()):
    """ weeks start at Monday """
    return date - datetime.timedelta(days=date.weekday())

def biweekly(date=datetime.date.today()):
    """ every two weeks """
    return datetime.date(date.year, date.month, 1 if date.day < 15 else 15)

def monthly(date=datetime.date.today()):
    """ Take a date object and return the first day of the month. """
    return datetime.date(date.year, date.month, 1)

def quarterly(date=datetime.date.today()):
    """ 1/1, 4/1, 7/1, 10/1 """
    return datetime.date(date.year, ((date.month - 1)//3) * 3 + 1, 1)

def semiyearly(date=datetime.date.today()):
    """ twice a year """
    return datetime.date(date.year, 1 if date.day < 7 else 7, 1)

def yearly(date=datetime.date.today()):
    """ once a year """
    return datetime.date(date.year, 1, 1)
