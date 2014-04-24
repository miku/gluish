# coding: utf-8

"""
Tests for Elasticsearch target and indexing.
"""

# pylint: disable=C0103,E1101,F0401
from gluish.esindex import ElasticsearchTarget, CopyToIndex
import elasticsearch
import luigi
import unittest


host = 'localhost'
port = 9200
index = 'luigi_test'
doc_type = 'test_type'
marker_index = 'luigi_test_index_updates'


def _create_test_index():
    """ Create content index, if if does not exists. """
    es = elasticsearch.Elasticsearch([{'host': host, 'port': port}])
    if not es.indices.exists(index):
        es.indices.create(index)


_create_test_index()
target = ElasticsearchTarget(host, port, index, doc_type, 'update_id')
target.marker_index = marker_index


class ElasticsearchTargetTest(unittest.TestCase):
    """ Test touch and exists. """
    def test_touch_and_exists(self):
        """ Basic test. """
        delete()
        self.assertFalse(target.exists(),
                         'Target should not exist before touching it')
        target.touch()
        self.assertTrue(target.exists(),
                        'Target should exist after touching it')
        delete()


def delete():
    """ Delete marker_index, if it exists. """
    es = elasticsearch.Elasticsearch([{'host': host, 'port': port}])
    if es.indices.exists(marker_index):
        es.indices.delete(marker_index)
    es.indices.refresh()


class CopyToTestIndex(CopyToIndex):
    """ Override the default `marker_index` table with a test name. """
    host = host
    port = port
    index = index
    doc_type = doc_type

    def output(self):
        """ Use a test target with an own marker_index. """
        target = ElasticsearchTarget(
            host=self.host,
            port=self.port,
            index=self.index,
            doc_type=self.doc_type,
            update_id=self.update_id()
         )
        target.marker_index = marker_index
        return target


class IndexingTask(CopyToTestIndex):
    """ Test the redundant version, where `_index` and `_type` are
    given in the `docs` as well. A more DRY example is `IndexingTask2`. """
    def docs(self):
        """ Return a list with a single doc. """
        return [{'_id': 123, '_index': self.index, '_type': self.doc_type,
                'name': 'sample', 'date': 'today'}]


class IndexingTask2(CopyToTestIndex):
    """ Just another task. """
    def docs(self):
        """ Return a list with a single doc. """
        return [{'_id': 234, '_index': self.index, '_type': self.doc_type,
                 'name': 'another', 'date': 'today'}]


class IndexingTask3(CopyToTestIndex):
    """ This task will request an empty index to start with. """
    purge_existing_index = True
    def docs(self):
        """ Return a list with a single doc. """
        return [{'_id': 234, '_index': self.index, '_type': self.doc_type,
                 'name': 'yet another', 'date': 'today'}]


def _cleanup():
    """ Delete both the test marker index and the content index. """
    es = elasticsearch.Elasticsearch([{'host': host, 'port': port}])
    if es.indices.exists(marker_index):
        es.indices.delete(marker_index)
    if es.indices.exists(index):
        es.indices.delete(index)


class CopyToIndexTest(unittest.TestCase):
    """ Test indexing tasks. """

    def setUp(self):
        """ Cleanup before starting. """
        _cleanup()

    def tearDown(self):
        """ Remove residues. """
        _cleanup()

    def test_copy_to_index(self):
        """ Test a single document upload. """
        task = IndexingTask()
        es = elasticsearch.Elasticsearch([{'host': host, 'port': port}])
        self.assertFalse(es.indices.exists(task.index))
        self.assertFalse(task.complete())
        luigi.build([task], local_scheduler=True)
        self.assertTrue(es.indices.exists(task.index))
        self.assertTrue(task.complete())
        self.assertEquals(1, es.count(index=task.index).get('count'))
        self.assertEquals({u'date': u'today', u'name': u'sample'},
                          es.get_source(index=task.index,
                                        doc_type=task.doc_type, id=123))

    def test_copy_to_index_incrementally(self):
        """ Test two tasks that upload docs into the same index. """
        task1 = IndexingTask()
        task2 = IndexingTask2()
        es = elasticsearch.Elasticsearch([{'host': host, 'port': port}])
        self.assertFalse(es.indices.exists(task1.index))
        self.assertFalse(es.indices.exists(task2.index))
        self.assertFalse(task1.complete())
        self.assertFalse(task2.complete())
        luigi.build([task1, task2], local_scheduler=True)
        self.assertTrue(es.indices.exists(task1.index))
        self.assertTrue(es.indices.exists(task2.index))
        self.assertTrue(task1.complete())
        self.assertTrue(task2.complete())
        self.assertEquals(2, es.count(index=task1.index).get('count'))
        self.assertEquals(2, es.count(index=task2.index).get('count'))

        self.assertEquals({u'date': u'today', u'name': u'sample'},
                          es.get_source(index=task1.index,
                                        doc_type=task1.doc_type, id=123))

        self.assertEquals({u'date': u'today', u'name': u'another'},
                          es.get_source(index=task2.index,
                                        doc_type=task2.doc_type, id=234))

    def test_copy_to_index_purge_existing(self):
        task1 = IndexingTask()
        task2 = IndexingTask2()
        task3 = IndexingTask3()
        es = elasticsearch.Elasticsearch([{'host': host, 'port': port}])
        luigi.build([task1, task2], local_scheduler=True)
        luigi.build([task3], local_scheduler=True)
        self.assertTrue(es.indices.exists(task3.index))
        self.assertTrue(task3.complete())
        self.assertEquals(1, es.count(index=task3.index).get('count'))

        self.assertEquals({u'date': u'today', u'name': u'yet another'},
                          es.get_source(index=task3.index,
                                        doc_type=task3.doc_type, id=234))
