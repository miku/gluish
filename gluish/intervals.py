# coding: utf-8

"""
Intervals for luigi.DateParameter.

Usage:

    # run this on the 1st of every month ...
    date = luigi.DateParameter(date=intervals.monthly())

"""

import datetime

def daily(d=datetime.date.today()):
    return d

def weekly(d=datetime.date.today()):
    return d - datetime.timedelta(days=d.weekday())

def biweekly(d=datetime.date.today()):
    """ every two weeks """
    return datetime.date(d.year, d.month, 1 if d.day < 15 else 15)

def monthly(d=datetime.date.today()):
    """ Take a date object and return the first day of the month. """
    return datetime.date(d.year, d.month, 1)

def quarterly(d=datetime.date.today()):
    """ 1/1, 4/1, 7/1, 10/1 """
    return datetime.date(d.year, ((d.month - 1)//3) * 3 + 1, 1)

def semiyearly(d=datetime.date.today()):
    return datetime.date(d.year, 1 if d.day < 7 else 7, 1)

def yearly(d=datetime.date.today()):
    return datetime.date(d.year, 1, 1)
