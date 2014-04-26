# coding: utf-8
"""
Common tests.
"""

# pylint:disable=F0401,C0111,W0232,E1101
from gluish.common import (LineCount, Executable, SplitFile, OAIHarvestChunk,
                           FTPMirror)
from gluish.path import unlink
from gluish.task import BaseTask
from gluish.utils import random_string
import datetime
import luigi
import os
import tempfile
import unittest
import BeautifulSoup

FIXTURES = os.path.join(os.path.dirname(__file__), 'fixtures')

class TestTask(BaseTask):
    BASE = tempfile.gettempdir()
    TAG = 't'


class External(TestTask, luigi.ExternalTask):
    filename = luigi.Parameter(default='l-1.txt')
    def output(self):
        print(os.path.join(FIXTURES, self.filename))
        return luigi.LocalTarget(path=os.path.join(FIXTURES, self.filename))


class ConcreteLineCount(TestTask, LineCount):
    filename = luigi.Parameter(default='l-1.txt')
    def requires(self):
        return External(filename=self.filename)

    def output(self):
        return luigi.LocalTarget(path=self.path())


class LineCountTest(unittest.TestCase):

    def test_run_l1(self):
        task = ConcreteLineCount(filename='l-1.txt')
        unlink(task.output().path)
        luigi.build([task], local_scheduler=True)
        content = task.output().open().read().strip()
        self.assertEquals('1', content)

    def test_run_l100(self):
        task = ConcreteLineCount(filename='l-100.txt')
        unlink(task.output().path)
        luigi.build([task], local_scheduler=True)
        content = task.output().open().read().strip()
        self.assertEquals('100', content)


class ExecutableTest(unittest.TestCase):

    def test_executable(self):
        task = Executable(name='ls')
        self.assertTrue(task.complete())

        task = Executable(name='veryunlikely123')
        self.assertFalse(task.complete())


class SplitFileTest(unittest.TestCase):

    def test_split_file(self):
        original = os.path.join(FIXTURES, 'l-100.txt')
        task = SplitFile(filename=original, chunks=10)
        unlink(task.output().path)

        luigi.build([task], local_scheduler=True)
        lines = [line.strip() for line in task.output().open()]
        self.assertEquals(10, len(lines))

        content = ''.join(open(fn).read() for fn in lines)
        with open(original) as handle:
            self.assertEquals(content, handle.read())


class SampleHarvestChunk(OAIHarvestChunk):
    """ Example harvesting. Will go out to the real server. """
    url = luigi.Parameter(default="http://oai.bnf.fr/oai2/OAIHandler")
    begin = luigi.DateParameter(default=datetime.date(2013, 1, 1))
    end = luigi.DateParameter(default=datetime.date(2013, 2, 1))
    prefix = luigi.Parameter(default="oai_dc")
    collection = luigi.Parameter(default="gallica:typedoc:partitions")

    def output(self):
        return luigi.LocalTarget(path=self.path())


class OAIHarvestChunkTest(unittest.TestCase):
    def test_harvest(self):
        task = SampleHarvestChunk()
        luigi.build([task], local_scheduler=True)
        want = BeautifulSoup.BeautifulStoneSoup(
            open(os.path.join(FIXTURES, 'sample_bnf_oai_response.xml')).read())
        got = BeautifulSoup.BeautifulStoneSoup(task.output().open().read())
        self.assertEquals(want.prettify(), got.prettify())


class MirrorTask(TestTask):
    """ Indicator make this task run on each test run. """
    indicator = luigi.Parameter(default=random_string())

    def requires(self):
        return FTPMirror(host='ftp.cs.brown.edu',
            username='anonymous',
            password='anonymous',
            pattern='*02*pdf',
            base='/pub/techreports/00')

    def run(self):
        luigi.File(self.input().path).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())


class FTPMirrorTest(unittest.TestCase):
    def test_ftp_mirror(self):
        task = MirrorTask()
        luigi.build([task], local_scheduler=True)
        got = task.output().open().read()
        self.assertTrue('cs00-02.pdf' in got,
                        msg='Task output was:\n%s' % got)
