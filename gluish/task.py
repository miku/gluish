#!/usr/bin/env python
# coding: utf-8
# pylint: disable=F0401,E1101,E1103

"""
Default task
============

A default task, that covers file system layout.
"""

from gluish.parameter import ClosestDateParameter
import datetime
import hashlib
import luigi
import os
import tempfile

__all__ = [
    'BaseTask',
    'MockTask'
]

def is_closest_date_parameter(task, param_name):
    """ Return the parameter class of param_name on task. """
    for name, obj in task.get_params():
        if name == param_name:
            return hasattr(obj, 'use_closest_date')
    return False

def delistify(x):
    """ A basic slug version of a given parameter list. """
    if isinstance(x, list):
        x = [e.replace("'", "") for e in x]
        return '-'.join(sorted(x))
    return x

class BaseTask(luigi.Task):
    """
    A base task with a `path` method. BASE should be set to the root
    directory of all tasks. TAG is a shard for a group of related tasks.
    """
    BASE = os.environ.get('GLUISH_DATA', tempfile.gettempdir())
    TAG = 'default'

    def closest(self):
        """ Return the closest date for a given date.
        Defaults to the same date. """
        if not hasattr(self, 'date'):
            raise AttributeError('Task has no date attribute.')
        return self.date

    def effective_task_id(self):
        """ Replace date in task id with closest date. """
        params = self.param_kwargs
        if 'date' in params and is_closest_date_parameter(self, 'date'):
            params['date'] = self.closest()
            task_id_parts = sorted(['%s=%s' % (k, str(v)) for k, v in params.items()])
            return '%s(%s)' % (self.task_family, ', '.join(task_id_parts))
        else:
            return self.task_id

    def taskdir(self):
        """ Return the directory under which all artefacts are stored. """
        return os.path.join(self.BASE, self.TAG, self.task_family)

    def path(self, filename=None, ext='tsv', digest=False, shard=False, encoding='utf-8'):
        """
        Return the path for this class with a certain set of parameters.
        `ext` sets the extension of the file.
        If `hash` is true, the filename (w/o extenstion) will be hashed.
        If `shard` is true, the files are placed in shards, based on the first
        two chars of the filename (hashed).
        """
        if self.BASE is NotImplemented:
            raise RuntimeError('BASE directory must be set.')

        params = dict(self.get_params())

        if filename is None:
            parts = []

            for name, param in self.get_params():
                if not param.significant:
                    continue
                if name == 'date' and is_closest_date_parameter(self, 'date'):
                    parts.append('date-%s' % self.closest())
                    continue
                if param.is_list:
                    es = '-'.join([str(v) for v in getattr(self, name)])
                    parts.append('%s-%s' % (name, es))
                    continue
                
                val = getattr(self, name)

                if isinstance(val, datetime.datetime):
                    val = val.strftime('%Y-%m-%dT%H%M%S')
                elif isinstance(val, datetime.date):
                    val = val.strftime('%Y-%m-%d')
                
                parts.append('%s-%s' % (name, val))

            name = '-'.join(sorted(parts))
            if len(name) == 0:
                name = 'output'
            if digest:
                name = hashlib.sha1(name.encode(encoding)).hexdigest()
            if not ext:
                filename = '{fn}'.format(ext=ext, fn=name)
            else:
                filename = '{fn}.{ext}'.format(ext=ext, fn=name)
            if shard:
                prefix = hashlib.sha1(filename.encode(encoding)).hexdigest()[:2]
                return os.path.join(self.BASE, self.TAG, self.task_family, prefix, filename)

        return os.path.join(self.BASE, self.TAG, self.task_family, filename)

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
