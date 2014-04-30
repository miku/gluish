# coding: utf-8

from gluish.format import TSV
from gluish.parameter import ILNParameter, AbstractMappedDateParameter
from gluish.task import BaseTask
from gluish.utils import random_string
import datetime
import luigi
import tempfile
import unittest


class MappedDateParameter1(AbstractMappedDateParameter):
    """ Simple static map. """
    def mapper(self, date):
        """ Map everything to a single date. """
        return datetime.date(1970, 1, 1)


class MappedDateParameter2(AbstractMappedDateParameter):
    """ Actually depend on the date parameter. """
    def mapper(self, date):
        """ Map everything to a week earlier. """
        return date - datetime.timedelta(days=7)


class TestTask(BaseTask):
    """ A base class for test tasks. """
    BASE = tempfile.gettempdir()
    TAG = 't'


class SomeTask(TestTask):
    """ A task that dumps a date. """
    indicator = luigi.Parameter(default=random_string())
    def run(self):
        with self.output().open('w') as output:
            output.write_tsv(datetime.date(2000, 1, 1))

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class MappedDateParameter3(AbstractMappedDateParameter):
    """ Use a SomeTask to find some date. """
    def mapper(self, date):
        task = SomeTask()
        luigi.build([task], local_scheduler=True)
        value = task.output().open().next().strip()
        return datetime.datetime.strptime(value, '%Y-%m-%d').date()

class TaskThatUsesAMappedParameter(TestTask):
    date = MappedDateParameter3(default=datetime.date.today())


class ParameterTest(unittest.TestCase):

    def test_iln_parameter(self):
        param = ILNParameter()
        self.assertEquals('0010', param.parse('10'))

    def test_mapped_parameter(self):
        param = MappedDateParameter1()
        self.assertEquals(datetime.date(1970, 1, 1), param.parse('2014-01-01'))

        param = MappedDateParameter2()
        self.assertEquals(datetime.date(2013, 12, 25), param.parse('2014-01-01'))

        param = MappedDateParameter3()
        self.assertEquals(datetime.date(2000, 1, 1), param.parse('2014-01-01'))


    def test_class_with_mapped_parameter(self):
        task = TaskThatUsesAMappedParameter()
        self.assertEquals(task.date, datetime.date(2000, 1, 1))
