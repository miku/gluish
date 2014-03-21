# coding: utf-8

"""
Parameter value rewriting example.
"""

from gluish.task import BaseTask
import datetime
import luigi
import tempfile


class ExampleBaseTask(BaseTask):
    """ Some overarching task, that only defines the root directory in BASE. """
    BASE = tempfile.gettempdir()
    TAG = 'group-1'


class SomeTask(ExampleBaseTask):
    """ Some concrete task from group one. """
    priority = luigi.IntParameter(default=10)
    date = luigi.DateParameter(default=datetime.date.today())

    def output(self):
        cbmap = {'date': lambda obj: datetime.date(self.date.year,
                                                   self.date.month, 1)}
        return luigi.LocalTarget(path=self.path(cbmap=cbmap))


if __name__ == '__main__':
    task1 = SomeTask(date=datetime.date(1970, 1, 30))
    task2 = SomeTask(date=datetime.date(1970, 1, 29))
    task3 = SomeTask(date=datetime.date(1970, 2, 1))

    # basename: date-1970-01-01-priority-10.tsv
    print(task1.output().path)
    print(task2.output().path)
    print(task3.output().path)
