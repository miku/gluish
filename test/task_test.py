# coding: utf-8
# pylint: disable=R0201

"""
Test tasks.
"""

# pylint: disable=E1101,W0232
from gluish import GLUISH_DATA
from gluish.task import BaseTask, MockTask, is_closest_date_parameter
from gluish.parameter import ClosestDateParameter
import unittest
import tempfile
import luigi
import datetime
import os

FIXTURES = os.path.join(os.path.dirname(__file__), 'fixtures')

# bring tempdir in line with GLUISH_DATA
tempfile.tempdir = os.environ.get(GLUISH_DATA, tempfile.gettempdir())


class TestTask(BaseTask):
    """ A base class for test tasks. """
    BASE = os.environ.get(GLUISH_DATA, tempfile.gettempdir())
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
    """ Task with a date param and an float param. And a closest method. """
    date = ClosestDateParameter(default=datetime.date(1970, 1, 1))
    threshold = luigi.FloatParameter(default=0.1)

    def closest(self):
        """ some dynamic attribute """
        return self.date

    def output(self):
        """ output """
        return luigi.LocalTarget(path=self.path())


class TaskE(TestTask):
    """ Task with a date param and an float param. And a closest method. """
    date = ClosestDateParameter(default=datetime.date(2000, 1, 1))
    threshold = luigi.FloatParameter(default=0.1)

    def closest(self):
        """ some dynamic attribute """
        return self.date - datetime.timedelta(days=365)

    def output(self):
        """ output """
        return luigi.LocalTarget(path=self.path())


class TaskF(TestTask):
    """ Task with a date param and an float param. And a closest method. """
    date = ClosestDateParameter(default=datetime.date(2000, 1, 1))
    threshold = luigi.FloatParameter(default=0.1, significant=False)

    def closest(self):
        """ some dynamic attribute """
        return self.date - datetime.timedelta(days=1)

    def output(self):
        """ output """
        return luigi.LocalTarget(path=self.path())


class TaskG(TestTask):
    """ Task with a date param and an float param. And a closest method. """
    date = luigi.DateParameter(default=datetime.date(2000, 1, 1))

    def output(self):
        """ output """
        return luigi.LocalTarget(path=self.path())


class TaskH(TestTask):
    """ Task with a date param and an float param. And a closest method. """
    closest = luigi.DateParameter(default=datetime.date(2000, 1, 1))

    def output(self):
        """ output """
        return luigi.LocalTarget(path=self.path())


class TaskI(TestTask):
    """ Task with a date param and an float param. And a closest method. """
    closest = ClosestDateParameter(default=datetime.date(2000, 1, 1))

    def closest(self):
        """ some dynamic attribute """
        return self.date - datetime.timedelta(days=1)

    def output(self):
        """ output """
        return luigi.LocalTarget(path=self.path())


class TaskJ(TestTask):
    """ Task with some exception in closest. """
    date = ClosestDateParameter(default=datetime.date(2000, 1, 1))

    def closest(self):
        raise ValueError()

    def output(self):
        """ output """
        return luigi.LocalTarget(path=self.path())


class TaskK(TestTask):
    """ Task that returns a float from closest. """
    date = ClosestDateParameter(default=datetime.date(2000, 1, 1))

    def closest(self):
        return 10.10

    def output(self):
        """ output """
        return luigi.LocalTarget(path=self.path())


class TaskL(TestTask):
    """ Task that returns some custom object from closest. """
    date = ClosestDateParameter(default=datetime.date(2000, 1, 1))

    def closest(self):
        class X(object):
            def __str__(self):
                return 'ABC'
        return X()

    def output(self):
        """ output """
        return luigi.LocalTarget(path=self.path())


class TaskM(TestTask):
    """ Task that returns a float from closest. """
    a = luigi.IntParameter(default=1)
    b = luigi.IntParameter(default=2)
    c = luigi.Parameter(default='hello')
    date = ClosestDateParameter(default=datetime.date(2000, 1, 1))

    def closest(self):
        return 10.10

    def output(self):
        """ output """
        return luigi.LocalTarget(path=self.path())


class TaskTest(unittest.TestCase):
    """ Test tasks. """

    def test_is_closest_date_parameter(self):
        self.assertEquals(is_closest_date_parameter(TaskL, 'date'), True)
        self.assertEquals(is_closest_date_parameter(TaskG, 'date'), False)

    def test_generic_task(self):
        """ Only output tests. """
        prefix = os.path.join(TestTask.BASE, TestTask.TAG)

        task = TaskA()
        self.assertEquals(task.output().path,
            os.path.join(prefix, 'TaskA', 'output.tsv'))

        task = TaskB()
        self.assertEquals(task.output().path,
            os.path.join(prefix, 'TaskB', 'date-1970-01-01.tsv'))

        task = TaskC()
        self.assertEquals(task.output().path,
            os.path.join(prefix, 'TaskC', 'date-1970-01-01-threshold-0.1.tsv'))

        task = TaskD()
        self.assertEquals(task.output().path,
            os.path.join(prefix, 'TaskD', 'date-1970-01-01-threshold-0.1.tsv'))

        task = TaskE()
        self.assertEquals(task.output().path,
            os.path.join(prefix, 'TaskE', 'date-1999-01-01-threshold-0.1.tsv'))

        task = TaskF()
        self.assertEquals(task.closest(), datetime.date(1999, 12, 31))
        self.assertEquals(task.output().path,
            os.path.join(prefix, 'TaskF', 'date-1999-12-31.tsv'))

        task = TaskG()
        self.assertEquals(task.closest(), datetime.date(2000, 1, 1))
        self.assertEquals(task.output().path,
            os.path.join(prefix, 'TaskG', 'date-2000-01-01.tsv'))

        task = TaskH()
        self.assertEquals(task.closest, datetime.date(2000, 1, 1))
        self.assertEquals(task.output().path,
            os.path.join(prefix, 'TaskH', 'closest-2000-01-01.tsv'))

        task = TaskI()
        with self.assertRaises(AttributeError):
            task.closest()

        self.assertEquals(task.output().path,
            os.path.join(prefix, 'TaskI', 'output.tsv'))

        task = TaskJ()
        with self.assertRaises(ValueError):
            task.closest()
        with self.assertRaises(ValueError):
            task.output().path

        task = TaskK()
        self.assertEquals(task.output().path,
            os.path.join(prefix, 'TaskK', 'date-10.1.tsv'))

        task = TaskL()
        self.assertEquals(task.output().path,
            os.path.join(prefix, 'TaskL', 'date-ABC.tsv'))

        
    def test_mock_task(self):
        """ Test the mock class. """
        task = MockTask(fixture=os.path.join(FIXTURES, 'l-1.txt'))
        self.assertEquals(task.content(), '1\n')
        luigi.build([task], local_scheduler=True)
        self.assertEquals(task.output().open().read(), '1\n')

    def test_effective_id(self):
        task = TaskK()
        self.assertEquals('TaskK(date=2000-01-01)', task.task_id)
        self.assertEquals('TaskK(date=10.1)', task.effective_task_id())

        task = TaskM()
        self.assertEquals('TaskM(a=1, b=2, c=hello, date=2000-01-01)',
                          task.task_id)
        self.assertEquals('TaskM(a=1, date=10.1, c=hello, b=2)',
                          task.effective_task_id())

        task = TaskG()
        self.assertEquals('TaskG(date=2000-01-01)', task.task_id)
        self.assertEquals('TaskG(date=2000-01-01)', task.effective_task_id())
