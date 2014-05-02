#!/usr/bin/env
# coding: utf-8

"""
Example FTPMirror task. On the command line:

    $ python ftpmirror.py MirrorTask --local-scheduler

Example output:

    /tmp/T/common/FTPMirror/e4a5338cb2000004928cf029e666a4024e40a798/cs00-02.pdf
    /tmp/T/common/FTPMirror/e4a5338cb2000004928cf029e666a4024e40a798/cs00-03.pdf
    /tmp/T/common/FTPMirror/e4a5338cb2000004928cf029e666a4024e40a798/cs00-06.pdf
    /tmp/T/common/FTPMirror/e4a5338cb2000004928cf029e666a4024e40a798/cs00-07.pdf
    /tmp/T/common/FTPMirror/e4a5338cb2000004928cf029e666a4024e40a798/cs00-08.pdf
"""

# pylint: disable=E1103
from __future__ import print_function
from gluish.common import FTPMirror
from gluish.task import BaseTask
from gluish.utils import random_string
import luigi
import tempfile


class DefaultTask(BaseTask):
    """ Some default abstract task for your tasks. BASE and TAG determine
    the paths, where the artefacts will be stored. """
    BASE = tempfile.gettempdir()
    TAG = 'just-a-test'


class MirrorTask(DefaultTask):
    """ Indicator make this task run on each test run. """
    indicator = luigi.Parameter(default=random_string())

    def requires(self):
        return FTPMirror(host='ftp.cs.brown.edu',
            username='anonymous', password='anonymous',
            pattern='*pdf', base='/pub/techreports/00')

    def run(self):
        self.input().move(self.output().path)
        print("Wrote receipt to: %s" % self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())


if __name__ == '__main__':
    luigi.run()
