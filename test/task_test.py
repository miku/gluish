# coding: utf-8
# pylint: disable=R0201,E1101,W0232,R0904
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
Test tasks.
"""

from gluish.parameter import ClosestDateParameter
from gluish.task import BaseTask, MockTask, is_closest_date_parameter
from gluish.utils import shellout
import datetime
import luigi
import os
import shutil
import tempfile
import unittest

FIXTURES = os.path.join(os.path.dirname(__file__), 'fixtures')

class TestTask(BaseTask):
    """ A base class for test tasks. """
    BASE = tempfile.gettempdir()
    TAG = 'gluish-testtasks'

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

class TaskN(TestTask):

    def run(self):
        """ Simulate touch. """
        luigi.File(path=self.output().path).open('w')

    def output(self):
        """ output """
        return luigi.LocalTarget(path=self.path())

class ShardedTask(TestTask):
    """ Example task, that shards its outputs. """
    param = luigi.Parameter(default='Hello')

    def run(self):
        """ Dummy run. """

    def output(self):
        """ Use shard=True """
        return luigi.LocalTarget(path=self.path(shard=True))

class TaskTest(unittest.TestCase):
    """ Test tasks. """

    @classmethod
    def tearDownClass(cls):
        base_dir = os.path.join(TestTask.BASE, TestTask.TAG)
        if os.path.exists(base_dir):
            shutil.rmtree(base_dir)

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
        self.assertRaises(AttributeError, task.closest)

        self.assertEquals(task.output().path,
            os.path.join(prefix, 'TaskI', 'output.tsv'))

        task = TaskJ()
        self.assertRaises(ValueError, task.closest)
        self.assertRaises(ValueError, task.output)

        task = TaskK()
        self.assertEquals(task.output().path,
            os.path.join(prefix, 'TaskK', 'date-10.1.tsv'))

        task = TaskL()
        self.assertEquals(task.output().path,
            os.path.join(prefix, 'TaskL', 'date-ABC.tsv'))

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
        """ Test effective_task_id """
        task = TaskK()
        self.assertTrue(task.task_id.startswith('TaskK_2000_01_01'))
        self.assertEquals('TaskK(date=10.1)', task.effective_task_id())

        task = TaskM()
        self.assertTrue(task.task_id.startswith('TaskM_1_2_hello_'))
        self.assertEquals('TaskM(a=1, b=2, c=hello, date=10.1)',
                          task.effective_task_id())

        task = TaskG()
        self.assertTrue(task.task_id.startswith('TaskG_2000_01_01_'))
        self.assertTrue(task.effective_task_id().startswith('TaskG_2000_01_01_'))

    def test_sharded_task(self):
        """ Test, if task output is sharded. """
        task = ShardedTask(param="Hello")
        self.assertTrue(task.output().path.endswith("62/param-Hello.tsv"))
        task = ShardedTask(param="Hi")
        self.assertTrue(task.output().path.endswith("1c/param-Hi.tsv"))

    def test_task_dir(self):
        task = TaskN()
        self.assertFalse(os.path.exists(task.taskdir()))
        luigi.build([task], local_scheduler=True)
        self.assertTrue(os.path.isdir(task.taskdir()))
        self.assertTrue(task.taskdir().endswith('TaskN'))
