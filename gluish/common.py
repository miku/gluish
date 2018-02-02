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
import json
import logging
import os

import luigi
import requests
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout

__all__ = ['Executable', 'Available', 'GitCloneRepository', 'GitUpdateRepository', 'CleanSolrIndex', 'FillSolrIndex']

logger = logging.getLogger('gluish')


def which(program):
    """
    Return `None` if no executable can be found.
    """

    def is_exe(fpath):
        """ Is `fpath` executable? ` """
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file

    return None


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


class CleanSolrIndex(luigi.Task):
    """
    Cleans a given Solr index from already existing data of a data package
    """
    date = ClosestDateParameter(default=datetime.date.today())
    solruri = luigi.Parameter()
    solrcore = luigi.Parameter()
    deletequery = luigi.Parameter()
    taskdir = luigi.Parameter()
    salt = luigi.Parameter()
    uritemplate = '{solruri}{solrcore}/update?commit=true'
    headers = {'Content-Type': 'text/xml'}
    payloadtemplate = '<delete><query>{deletequery}</query></delete>'

    def requires(self):
        return Available(service=self.solruri,
                         message="provide a running Solr, please")

    def run(self):
        uri = self.uritemplate.format(solruri=self.solruri,
                                      solrcore=self.solrcore)
        payload = self.payloadtemplate.format(deletequery=self.deletequery)
        result = {}

        try:
            response = requests.post(uri, data=payload, headers=self.headers)

            result['status_code'] = response.status_code
            result['content'] = response.content.decode('utf-8')

            if result['status_code'] != 200:
                raise RuntimeError("""could not successfully clean Solr core '%s' (status code = '%d')""" % (
                    uri, response.status_code))

            if not str(result['content']).startswith(
                    """<?xml version="1.0" encoding="UTF-8"?>\n<response>\n<lst name="responseHeader"><int name="status">0</int>"""):
                raise RuntimeError(
                    """could not successfully clean Solr core '%s' (response = '%s')""" % (uri, result['content']))
        except requests.exceptions.ConnectionError as error:
            result['status_code'] = 404
            result['error'] = str(error).replace('"', '\\"')

            raise RuntimeError("""could not successfully establish connection to Solr index '%s'""" % uri)
        finally:
            with self.output().open('w') as outfile:
                outfile.write(json.dumps(result, indent=None) + "\n")

    def output(self):
        filename = 'response_' + str(self.salt) + '.json'
        return luigi.LocalTarget(path=os.path.join(self.taskdir, filename))

    def complete(self):
        # TODO maybe define a better complete criteria (?) (background: complete() will be executed before run())
        # e.g. check, whether something of this source is in the index
        # or move all the stuff from run to complete (?)
        if not self.output().exists():
            return False

        with self.output().open('r') as resultfile:
            result = json.loads(resultfile.read())

            if result['status_code'] != 200:
                return False

            if not str(result['content']).startswith(
                    """<?xml version="1.0" encoding="UTF-8"?>\n<response>\n<lst name="responseHeader"><int name="status">0</int>"""):
                return False

            return True


class FillSolrIndex(luigi.Task):
    # TODO: define proper complete criteria (?)
    # e.g. check, whether the amount of records that should be loaded into the index is index (if not, then index load is not successfully)
    """
    Loads processed data of a data package into a given Solr index (with help of solrbulk)
    """
    date = ClosestDateParameter(default=datetime.date.today())
    solruri = luigi.Parameter()
    solrcore = luigi.Parameter()
    deletequery = luigi.Parameter()
    input = luigi.Parameter()
    taskdir = luigi.Parameter()
    outputfilename = luigi.Parameter()
    salt = luigi.Parameter()

    def requires(self):
        return [
            Available(service=self.solruri,
                      message="provide a running Solr, please"),
            Executable(name='solrbulk'),
            CleanSolrIndex(date=self.date,
                           solruri=self.solruri,
                           solrcore=self.solrcore,
                           deletequery=self.deletequery,
                           taskdir=self.taskdir,
                           salt=self.salt)
        ]

    def run(self):
        server = str(self.solruri) + str(self.solrcore)
        inputpath = self.input
        output = shellout(
            """solrbulk -verbose -server {server} -w 2 < {input}""",
            server=server,
            input=inputpath)
        luigi.LocalTarget(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.taskdir, str(self.outputfilename)))
