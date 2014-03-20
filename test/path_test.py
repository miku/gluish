# coding: utf-8

from gluish.path import (unlink, size_fmt, wc, filehash, touch, which,
                         findfiles, fileage, copyregions, random_tmp_path)
import unittest
import tempfile
import os
import stat
import errno

class PathTest(unittest.TestCase):

    def test_unlink(self):
        """ Should not raise any exception on non-existent file. """
        path = tempfile.mktemp()
        self.assertFalse(os.path.exists(path))

        with self.assertRaises(OSError) as context:
            os.unlink(path)
        self.assertEquals(errno.ENOENT, context.exception.errno)
        
        exception_raised = False
        try:
            unlink(path)
        except Exception as exc:
            exception_raised = True
        self.assertFalse(exception_raised)

    def test_size_fmt(self):
        self.assertEquals('1.0b', size_fmt(1))
        self.assertEquals('1.0K', size_fmt(1024 ** 1))
        self.assertEquals('1.0M', size_fmt(1024 ** 2))
        self.assertEquals('1.0G', size_fmt(1024 ** 3))
        self.assertEquals('1.0T', size_fmt(1024 ** 4))
        self.assertEquals('1.0P', size_fmt(1024 ** 5))
        self.assertEquals('1.0E', size_fmt(1024 ** 6))
        self.assertEquals('1023.0b', size_fmt(1023))
        self.assertEquals('184.7G', size_fmt(198273879123))

    def test_wc(self):
        with tempfile.NamedTemporaryFile(delete=False) as handle:
            handle.write('Line 1\n')
            handle.write('Line 2\n')
            handle.write('Line 3\n')
        self.assertEquals(3, wc(handle.name))

    def test_filehash(self):
        with tempfile.NamedTemporaryFile(delete=False) as handle:
            handle.write('0123456789\n')
        self.assertEquals('3a30948f8cd5655fede389d73b5fecd91251df4a',
                          filehash(handle.name))

    def test_touch(self):
        path = tempfile.mktemp()
        self.assertFalse(os.path.exists(path))
        touch(path)
        self.assertTrue(os.path.exists(path))

    def test_which(self):
        self.assertIsNotNone(which('ls'))
        self.assertEquals('/bin/ls', which('ls'))
        self.assertIsNone(which('veryunlikely1234'))

    def test_findfiles(self):
        directory = tempfile.mkdtemp()
        subdir = tempfile.mkdtemp(dir=directory)
        touch(os.path.join(directory, '1.jpg'))
        touch(os.path.join(subdir, '2-1.txt'))
        touch(os.path.join(subdir, '2-2.txt'))

        pathlist = list(findfiles(directory))
        self.assertEquals(3, len(pathlist))
        self.assertEquals(['1.jpg', '2-1.txt', '2-2.txt'], map(os.path.basename, pathlist))
        self.assertEquals('{}/1.jpg'.format(directory), pathlist[0])
        self.assertEquals('{}/2-1.txt'.format(subdir), pathlist[1])
        self.assertEquals('{}/2-2.txt'.format(subdir), pathlist[2])

        pathlist = list(findfiles(directory, fun=lambda path: path.endswith('jpg')))
        self.assertEquals(1, len(pathlist))
        self.assertEquals('{}/1.jpg'.format(directory), pathlist[0])

    def test_fileage(self):
        """ Assume we can `touch` within 2000ms. """
        path = tempfile.mktemp()
        self.assertFalse(os.path.exists(path))
        touch(path)
        self.assertTrue(os.path.exists(path))
        self.assertTrue(0 < fileage(path) < 2)

    def test_copyregions(self):
        with tempfile.NamedTemporaryFile(delete=False) as handle:
            handle.write('0123456789\n')

        with open(handle.name) as src:
            with tempfile.NamedTemporaryFile(delete=False) as dst:
                copyregions(src, dst, [(3, 2)])

        with open(dst.name) as handle:
            self.assertEquals("34", handle.read())

    def test_random_tmp_path(self):
        path = random_tmp_path()
        self.assertEquals(23, len(os.path.basename(path)))
