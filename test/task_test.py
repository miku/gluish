# coding: utf-8
# pylint: disable=R0201

"""
Test tasks.
"""

from gluish.task import BaseTask, nearest, MockTask
import unittest
import tempfile
import luigi
import datetime
import os

FIXTURES = os.path.join(os.path.dirname(__file__), 'fixtures')


class TestTask(BaseTask):
    """ A base class for test tasks. """
    BASE = tempfile.gettempdir()
    TAG = 't'


class TaskA(TestTask):
    """ Plain vanilla task, that does nothing. """

    def output(self):
        """ output """
        return luigi.LocalTarget(path=self.path())


class TaskB(TestTask):
    """ Task with a date param. """
    date = luigi.DateParameter(default=datetime.date(1970, 1, 1))

    def output(self):
        """ output """
        return luigi.LocalTarget(path=self.path())


class TaskC(TestTask):
    """ Task with a date param and an float param. """
    date = luigi.DateParameter(default=datetime.date(1970, 1, 1))
    threshold = luigi.FloatParameter(default=0.1)

    def output(self):
        """ output """
        return luigi.LocalTarget(path=self.path())


class TaskD(TestTask):
    """ Task with a date param and an float param. And a value rewriter. """
    date = luigi.DateParameter(default=datetime.date(1970, 1, 1))
    threshold = luigi.FloatParameter(default=0.1)

    def nearest(self):
        """ some dynamic attribute """
        return '1234'

    def output(self):
        """ output """
        cbmap = {'date': lambda obj: obj.nearest()}
        return luigi.LocalTarget(path=self.path(cbmap=cbmap))


class TaskE(TestTask):
    """ Task with a date param and an insignificant float param. """
    date = luigi.DateParameter(default=datetime.date(1970, 1, 1))
    threshold = luigi.FloatParameter(default=0.1, significant=False)

    def nearest(self):
        """ some dynamic attribute """
        return '1234'

    def output(self):
        """ output """
        cbmap = {'date': lambda obj: obj.nearest()}
        return luigi.LocalTarget(path=self.path(cbmap=cbmap))


class TaskF(TestTask):
    """ Some custom cbmap/rewriter. """
    date = luigi.DateParameter(default=datetime.date(1970, 1, 1))
    city = luigi.Parameter(default='Madrid')
    airport = luigi.Parameter(default='Barajas')
    tags = luigi.Parameter(default='')

    def get_tags(self):
        """ make some external query ... """
        return '-'.join(('hot', 'dry'))

    def output(self):
        """ output """
        cbmap = {'tags': lambda obj: obj.get_tags()}
        return luigi.LocalTarget(path=self.path(cbmap=cbmap))


class TaskG(TestTask):
    """ Another rewrite example. """
    date = luigi.DateParameter(default=datetime.date(1970, 1, 1))

    def nearest(self):
        """ some dynamic attribute """
        return '1.1.1970'

    def output(self):
        """ output and rewrite """
        cbmap = {'date': nearest}
        return luigi.LocalTarget(path=self.path(cbmap=cbmap))


class TaskTest(unittest.TestCase):
    """ Test tasks. """

    def test_generic_task(self):
        """ Only output tests. """
        task = TaskA()
        self.assertEquals(task.output().path,
            os.path.join(TestTask.BASE, TestTask.TAG, 'TaskA', 'output.tsv'))

        task = TaskB()
        self.assertEquals(task.output().path,
            os.path.join(TestTask.BASE, TestTask.TAG, 'TaskB',
                         'date-1970-01-01.tsv'))

        task = TaskC()
        self.assertEquals(task.output().path,
            os.path.join(TestTask.BASE, TestTask.TAG, 'TaskC',
                         'date-1970-01-01-threshold-0.1.tsv'))

        task = TaskD()
        self.assertEquals(task.output().path,
            os.path.join(TestTask.BASE, TestTask.TAG, 'TaskD',
                         'date-1234-threshold-0.1.tsv'))

        task = TaskE()
        self.assertEquals(task.output().path,
            os.path.join(TestTask.BASE, TestTask.TAG, 'TaskE',
                         'date-1234.tsv'))

        task = TaskF()
        self.assertEquals(task.output().path,
            os.path.join(TestTask.BASE, TestTask.TAG, 'TaskF',
                'airport-Barajas-city-Madrid-date-1970-01-01-tags-hot-dry.tsv'))

        task = TaskG()
        self.assertEquals(task.output().path,
            os.path.join(TestTask.BASE, TestTask.TAG, 'TaskG',
                         'date-1.1.1970.tsv'))


    def test_mock_task(self):
        """ Test the mock class. """
        task = MockTask(fixture=os.path.join(FIXTURES, 'l-1.txt'))
        self.assertEquals(task.content(), '1\n')
        luigi.build([task], local_scheduler=True)
        self.assertEquals(task.output().open().read(), '1\n')

