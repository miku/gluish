#!/usr/bin/env python
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

import datetime
import logging
import os

import luigi
import requests

from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout, which

__all__ = ['Executable', 'Available', 'GitCloneRepository', 'GitUpdateRepository', 'FillSolrIndex']

logger = logging.getLogger('gluish')


def service_is_up(service):
    """
    Return `False` if HTTP services returns status code != 200.
    """
    try:
        return requests.get(service).status_code == 200
    except:
        return False


def getfirstline(file, default):
    """
    Returns the first line of a file.
    """
    with open(file, 'rb') as fh:
        content = fh.readlines()
        if len(content) == 1:
            return content[0].decode('utf-8').strip('\n')

    return default


class chdir(object):
    """
    Change directory temporarily.
    """

    def __init__(self, path):
        self.wd = os.getcwd()
        self.dir = path

    def __enter__(self):
        os.chdir(self.dir)

    def __exit__(self, *args):
        os.chdir(self.wd)


class Executable(luigi.Task):
    """
    Checks, whether an external executable is available. This task will consider
    itself complete, only if the executable `name` is found in $PATH.
    """
    name = luigi.Parameter()
    message = luigi.Parameter(default="")

    def run(self):
        """ Only run if, task is not complete. """
        raise RuntimeError('external program required: %s (%s)' % (self.name, self.message))

    def complete(self):
        return which(self.name) is not None


class Available(luigi.Task):
    """
    Checks, whether an HTTP service is available or not. This task will consider
    itself complete, only if the HTTP service return a 200.
    """
    service = luigi.Parameter()
    message = luigi.Parameter(default="")

    def run(self):
        """ Only run if, task is not complete. """
        raise RuntimeError('HTTP service %s is not available (%s)' % (self.service, self.message))

    def complete(self):
        return service_is_up(self.service)


class GitCloneRepository(luigi.Task):
    """
    Checks, whether a certain directory already exists (that should contain a Git repository) - otherwise it will be cloned.
    """
    gitrepository = luigi.Parameter()
    repositorydirectory = luigi.Parameter()
    basedirectory = luigi.Parameter()

    def requires(self):
        return Executable(name='git')

    def run(self):
        self.output().makedirs()
        with chdir(str(self.basedirectory)):
            shellout("""git clone {gitrepository}""", gitrepository=self.gitrepository)

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.basedirectory, str(self.repositorydirectory)))


class GitUpdateRepository(luigi.Task):
    """
    Updates an existing Git repository
    """
    gitrepository = luigi.Parameter()
    repositorydirectory = luigi.Parameter()
    basedirectory = luigi.Parameter()
    branch = luigi.Parameter(default="master", significant=False)

    def requires(self):
        return [
            GitCloneRepository(gitrepository=self.gitrepository,
                               repositorydirectory=self.repositorydirectory,
                               basedirectory=self.basedirectory),
            Executable(name='git')
        ]

    def run(self):
        with chdir(str(self.output().path)):
            shellout("""git checkout {branch}""", branch=self.branch)
            shellout("""git pull origin {branch}""", branch=self.branch)

    def complete(self):
        if not self.output().exists():
            return False

        with chdir(str(self.output().path)):
            output = shellout("""git fetch origin {branch} > {output} 2>&1""", branch=self.branch)

            result = True

            with open(output, 'rb') as fh:
                content = fh.readlines()
                if len(content) >= 3:
                    result = False

            revparseoutput = shellout("""git rev-parse {branch} > {output} 2>&1""", branch=self.branch)
            originrevparseoutput = shellout("""git rev-parse origin/{branch} > {output} 2>&1""", branch=self.branch)

            revparse = getfirstline(revparseoutput, "0")
            originrevparse = getfirstline(originrevparseoutput, "1")

            if revparse != originrevparse:
                result = False

        return result

    def output(self):
        return luigi.LocalTarget(path=os.path.join(str(self.basedirectory), str(self.repositorydirectory)))


class FillSolrIndex(luigi.Task):
    # TODO: define proper complete criteria (?)
    # e.g. check, whether the amount of records that should be loaded into the index is index (if not, then index load is not successfully)
    """
    Loads processed data of a data package into a given Solr index (with help of solrbulk)
    """
    date = ClosestDateParameter(default=datetime.date.today())
    solruri = luigi.Parameter()
    solrcore = luigi.Parameter()
    purge = luigi.BoolParameter(default=False, significant=False)
    purgequery = luigi.Parameter(default="", significant=False)
    size = luigi.IntParameter(default=1000, significant=False)
    worker = luigi.IntParameter(default=2, significant=False)
    commitlimit = luigi.IntParameter(default=1000, significant=False)
    input = luigi.Parameter()
    taskdir = luigi.Parameter()
    outputfilename = luigi.Parameter()
    salt = luigi.Parameter()

    def determineprefix(self, purge=None, purgequery=None):
        solrbulk = 'solrbulk'

        if purge and purgequery is not None:
            return solrbulk + ' -purge -purge-query "' + purgequery + '"'
        if purge:
            return solrbulk + ' -purge'

        return solrbulk

    def requires(self):
        return [
            Available(service=self.solruri,
                      message="provide a running Solr, please"),
            Executable(name='solrbulk',
                       message='solrbulk command is missing on your system, you can, e.g., install it as a deb package on your Debian-based linux system (see https://github.com/miku/solrbulk#installation)'),
        ]

    def run(self):
        prefix = self.determineprefix(self.purge, self.purgequery)
        server = str(self.solruri) + str(self.solrcore)
        chunksize = self.size
        cores = self.worker
        inputpath = self.input
        commit = self.commitlimit
        output = shellout(
            """{prefix} -verbose -server {server} -size {size} -w {worker} -commit {commit} < {input}""",
            prefix=prefix,
            server=server,
            size=chunksize,
            worker=cores,
            commit=commit,
            input=inputpath)

        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.taskdir, str(self.outputfilename)))
