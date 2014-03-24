#!/usr/bin/env python
# coding: utf-8

"""
Default task
============

A default task, that covers file system layout.

"""
# pylint: disable=F0401,E1101,E1103
from luigi.task import id_to_name_and_params
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

    def path(self, filename=None, ext='tsv', digest=False, cbmap=None):
        """
        Return the path for this class with a certain set of parameters.

        `ext` sets the extension of the file.

        If `hash` is true, the filename (w/o extenstion) will be hashed.

        `cbmap` is a callback dictionary, with parameter names as keys and
        function objects as values. A use case is the mapping of a date to some
        nearest date.

        The example below is a task, that can be called with any date,
        but that maps the given date to the first day of the given month.

        Basically every day of a single month will report the same output
        file.

        class ExampleTask(BaseTask):
            # ...
            date = luigi.DateParameter(default=datetime.date.today())

            def nearest(self):
                ''' Map any given date to the first day of the month. '''
                return datetime.date(self.date.year, self.date.month, 1)

            def output(self):
                cbmap = {'date': lambda obj: return obj.nearest()}
                return luigi.LocalTarget(path=self.path(cbmap=cbmap))

            """
        if self.TAG is NotImplemented or self.BASE is NotImplemented:
            raise RuntimeError('TAG and BASE must be set.')
        if cbmap is None:
            cbmap = {}

        task_name, task_params = id_to_name_and_params(self.task_id)

        if filename is None:
            for key, callback in cbmap.iteritems():
                if not key in task_params:
                    continue
                task_params[key] = callback(self)

            parts = ('{k}-{v}'.format(k=k, v=v)
                     for k, v in task_params.iteritems())

            name = '-'.join(sorted(parts))
            if len(name) == 0:
                name = 'output'
            if digest:
                name = hashlib.sha1(name).hexdigest()
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

