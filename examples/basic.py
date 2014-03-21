# coding: utf-8

"""
Basic usage of BaseTask.
"""

from gluish.task import BaseTask
import datetime
import luigi
import tempfile


class ProjectBaseTask(BaseTask):
    """ Some overarching task, that only defines the root directory in BASE. """
    BASE = tempfile.gettempdir()


class GroupOne(ProjectBaseTask):
    """ A group of related tasks. """
    TAG = 'group-1'


class GroupTwo(ProjectBaseTask):
    """ A group of related tasks. """
    TAG = 'group-2'


class SomeTask(GroupOne):
    """ Some concrete task from group one. """
    priority = luigi.IntParameter(default=10)

    def output(self):
        return luigi.LocalTarget(path=self.path())


class AnotherTask(GroupTwo):
    """ Some concrete task from group two. """
    priority = luigi.IntParameter(default=10)
    date = luigi.DateParameter(default=datetime.date.today())
    name = luigi.Parameter(default='No', significant=False)

    def output(self):
        return luigi.LocalTarget(path=self.path())


if __name__ == '__main__':
    task1 = SomeTask()
    task2 = AnotherTask()

    print(task1.output().path)
    print(task2.output().path)
