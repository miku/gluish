#!/usr/bin/env python
# coding: utf-8

"""
Default task
============

A default task, that covers file system layout.

"""
# pylint: disable=F0401,E1101,E1103
from luigi.task import id_to_name_and_params
import datetime
import hashlib
import luigi
import os
import tempfile


def nearest(obj):
    """
    Replace a parameter named `date` with the value of a `nearest` attribute,
    which should return the nearest date in the past.
    """
    if not hasattr(obj, 'nearest'):
        raise AttributeError('Missing attribute.')
    return obj.nearest() if callable(obj.nearest) else obj.nearest


def default_base(envvar='TASKHOME'):
    """ Let the environment control the base path for all tasks. """
    return os.environ.get(envvar, tempfile.gettempdir())


class BaseTask(luigi.Task):
    """
    A base task with a `path` method. BASE should be set to the root
    directory of all tasks. TAG is a shard for a group of related tasks.
    """
    BASE = default_base()
    TAG = 'default'

    def closest(self):
        """ Return the closest date for a given date.
        Defaults to the same date. """
        return self.date

    def path(self, filename=None, ext='tsv', digest=False):
        """
        Return the path for this class with a certain set of parameters.
        `ext` sets the extension of the file.
        If `hash` is true, the filename (w/o extenstion) will be hashed.
        """
        if self.TAG is NotImplemented or self.BASE is NotImplemented:
            raise RuntimeError('TAG and BASE must be set.')
        
        task_name, task_params = id_to_name_and_params(self.task_id)

        if filename is None:
            # use `self.closest`
            if 'date' in task_params:
                if (isinstance(self.date, datetime.date) and
                    hasattr(self, 'closest')):
                    task_params['date'] = str(self.closest())

            parts = ('{k}-{v}'.format(k=k, v=v)
                     for k, v in task_params.iteritems())

            name = '-'.join(sorted(parts))
            if len(name) == 0:
                name = 'output'
            if digest:
                name = hashlib.sha1(name).hexdigest()
            if not ext:
                filename = '{fn}'.format(ext=ext, fn=name)
            else:
                filename = '{fn}.{ext}'.format(ext=ext, fn=name)

        return os.path.join(unicode(self.BASE), unicode(self.TAG), task_name,
                            filename)


class MockTask(BaseTask):
    """ A mock task object. Read fixture from path and that's it. """
    fixture = luigi.Parameter()

    def content(self):
        """ Return the content of the file in path. """
        with open(self.fixture) as handle:
            return handle.read()

    def run(self):
        """ Just copy the fixture, so we have some output. """
        luigi.File(path=self.fixture).copy(self.output().path)

    def output(self):
        """ Mock output. """
        return luigi.LocalTarget(path=self.path(digest=True))

