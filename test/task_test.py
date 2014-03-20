# coding: utf-8

from gluish.task import BaseTask, nearest
import unittest
import tempfile
import luigi
import datetime
import os


class TestTask(BaseTask):
    BASE = tempfile.gettempdir()
    TAG = 't'

class A(TestTask):
    def output(self):
        return luigi.LocalTarget(path=self.path())


class B(TestTask):
    date = luigi.DateParameter(default=datetime.date(1970, 1, 1))

    def output(self):
        return luigi.LocalTarget(path=self.path())


class C(TestTask):
    date = luigi.DateParameter(default=datetime.date(1970, 1, 1))
    threshold = luigi.FloatParameter(default=0.1)

    def output(self):
        return luigi.LocalTarget(path=self.path())


class D(TestTask):
    date = luigi.DateParameter(default=datetime.date(1970, 1, 1))
    threshold = luigi.FloatParameter(default=0.1)

    def nearest(self):
        return '1234'

    def output(self):
        cbmap = {'date': lambda obj: obj.nearest()}
        return luigi.LocalTarget(path=self.path(cbmap=cbmap))


class E(TestTask):
    date = luigi.DateParameter(default=datetime.date(1970, 1, 1))
    threshold = luigi.FloatParameter(default=0.1, significant=False)

    def nearest(self):
        return '1234'

    def output(self):
        cbmap = {'date': lambda obj: obj.nearest()}
        return luigi.LocalTarget(path=self.path(cbmap=cbmap))

class F(TestTask):
    date = luigi.DateParameter(default=datetime.date(1970, 1, 1))
    city = luigi.Parameter(default='Madrid')
    airport = luigi.Parameter(default='Barajas')
    tags = luigi.Parameter(default='')

    def tags(self):
        # make some external query
        return '-'.join(('hot', 'dry', 'sunny', 'rainy'))

    def output(self):
        cbmap = {'date': lambda obj: obj.tags()}
        return luigi.LocalTarget(path=self.path(cbmap=cbmap))

class G(TestTask):
    date = luigi.DateParameter(default=datetime.date(1970, 1, 1))

    def nearest(self):
        return '1.1.1970'

    def output(self):
        cbmap = {'date': nearest}
        return luigi.LocalTarget(path=self.path(cbmap=cbmap))        

class TaskTest(unittest.TestCase):

    def test_generic_task(self):
        task = A()
        self.assertEquals(task.output().path,
            os.path.join(TestTask.BASE, TestTask.TAG, 'A', 'output.tsv'))

        task = B()
        self.assertEquals(task.output().path,
            os.path.join(TestTask.BASE, TestTask.TAG, 'B', 'date-1970-01-01.tsv'))

        task = C()
        self.assertEquals(task.output().path,
            os.path.join(TestTask.BASE, TestTask.TAG, 'C', 'date-1970-01-01-threshold-0.1.tsv'))

        task = D()
        self.assertEquals(task.output().path,
            os.path.join(TestTask.BASE, TestTask.TAG, 'D', 'date-1234-threshold-0.1.tsv'))

        task = E()
        self.assertEquals(task.output().path,
            os.path.join(TestTask.BASE, TestTask.TAG, 'E', 'date-1234.tsv'))

        task = F()
        self.assertEquals(task.output().path,
            os.path.join(TestTask.BASE, TestTask.TAG, 'F', 'airport-Barajas-city-Madrid-date-hot-dry-sunny-rainy.tsv'))

        task = G()
        self.assertEquals(task.output().path,
            os.path.join(TestTask.BASE, TestTask.TAG, 'G', 'date-1.1.1970.tsv'))
