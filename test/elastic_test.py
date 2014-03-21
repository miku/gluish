# coding: utf-8

"""
Unit test for ES indexing task. Does not require ES to run.
"""

from gluish.elastic import ESIndexTask
import elasticsearch
import unittest
import requests


class ESTestTask(ESIndexTask):
    """ A test task with a very unlikely index name. """
    index = 'test-033261f98b7bf9b1c624908eb8b62c2f'
    doc_type = 'test'

    def docs(self):
        return []

class ElasticTest(unittest.TestCase):
    """ ES tests. """

    def test_elastic(self):
        """ Tests w/o integration. """
        es = elasticsearch.Elasticsearch()
        task = ESTestTask()
        self.assertEquals({}, task.settings)
        self.assertEquals({}, task.mapping)
        self.assertEquals(0, task.expected())

        if es.ping():
            self.assertFalse(task.exists())

        if es.ping():
            exception_raised = False
            try:
                self.assertEquals(0, task.indexed())
            except elasticsearch.NotFoundError:
                exception_raised = True
            self.assertTrue(exception_raised)

        self.assertEquals('test', task.doc_type)
        self.assertEquals('test-033261f98b7bf9b1c624908eb8b62c2f', task.index)

        self.assertEquals(1, len(task.hosts))
        self.assertEquals('localhost', task.hosts[0].get('host'))
        self.assertEquals(9200, task.hosts[0].get('port'))

        if es.ping():
            self.assertFalse(task.complete())