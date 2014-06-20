#!/usr/bin/env python
# coding: utf-8

"""
Provides a basic benchmark decorator. Usage:

    @timed
    def run(self):
        print('Running...')

Just logs the output. Could be extended to send this information to some service
if available.
"""

from gluish.colors import green, yellow, red
from gluish.database import sqlite3db
from timeit import default_timer
import functools
import logging
import os

logger = logging.getLogger('gluish')


class Timer(object):
    """ A timer as a context manager. """

    def __init__(self, green=10, yellow=60):
        """ Indicate runtimes with colors, green < 10s, yellow < 60s. """
        self.timer = default_timer
        # measures wall clock time, not CPU time!
        # On Unix systems, it corresponds to time.time
        # On Windows systems, it corresponds to time.clock

        self.green = green
        self.yellow = yellow

    def __enter__(self):
        self.start = self.timer() # measure start time
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.end = self.timer() # measure end time
        self.elapsed_s = self.end - self.start # elapsed time, in seconds
        self.elapsed_ms = self.elapsed_s * 1000  # elapsed time, in milliseconds


def timed(method):
    """ A @timed decorator. """
    @functools.wraps(method)
    def _timed(*args, **kwargs):
        """ Benchmark decorator. """
        with Timer() as timer:
            result = method(*args, **kwargs)
        klass = args[0].__class__.__name__
        fun = method.__name__

        msg = '[%s.%s] %0.5f' % (klass, fun, timer.elapsed_s)
        if timer.elapsed_s <= timer.green:
            logger.debug(green(msg))
        elif timer.elapsed_s <= timer.yellow:
            logger.debug(yellow(msg))
        else:
            logger.debug(red(msg))
        return result
    return _timed


DEFAULT_BENCHMARK_DB = os.path.join(os.path.expanduser('~'),
                                    '.timedb', 'time.db')

def ptimed(method=None, path=DEFAULT_BENCHMARK_DB, log=True):
    """
    A persistent timer. Will write results into a TSV file under
    $HOME/.timed/data.tsv
    """
    if method is None:
        return functools.partial(ptimed, path=path, log=log)
    @functools.wraps(method)
    def decorator(*args, **kwargs):
        """ Benchmark decorator. """
        with Timer() as timer:
            result = method(*args, **kwargs)

        module = args[0].__module__
        klass = args[0].__class__.__name__
        fun = method.__name__

        key = '{}.{}.{}'.format(module, klass, fun)
        value = timer.elapsed_s
        # just a quick visual impression, everything that takes more
        # than 10s is yellow, more then 1min, red.
        status = 'green'
        if value > 10:
            status = 'yellow'
        if value > 60:
            status = 'red'

        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        with sqlite3db(path) as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS
                t (key TEXT, value REAL, date TEXT, status TEXT)
            """)
            cursor.execute("""
                INSERT INTO t (key, value, date, status)
                VALUES (?, ?, datetime('now'), ?)
            """, (key, value, status))

        if log is True:
            msg = '[%s.%s] %0.5f' % (klass, fun, timer.elapsed_s)
            if timer.elapsed_s <= timer.green:
                logger.debug(green(msg))
            elif timer.elapsed_s <= timer.yellow:
                logger.debug(yellow(msg))
            else:
                logger.debug(red(msg))

        return result
    return decorator
