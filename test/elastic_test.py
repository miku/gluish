# coding: utf-8

"""
Unit test for ES indexing task. Does not require ES to run.
"""

from gluish.elastic import ESIndexTask
import elasticsearch
import unittest


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
        task = ESTestTask()
        self.assertEquals({}, task.settings)
        self.assertEquals({}, task.mapping)
        self.assertEquals(0, task.expected())
        self.assertFalse(task.exists())


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

        self.assertFalse(task.complete())