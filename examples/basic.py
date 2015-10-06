# coding: utf-8
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
