# coding: utf-8

from gluish.format import write_tsv, iter_tsv, TSV
import luigi
import unittest
import tempfile

class FormatTest(unittest.TestCase):

    def test_write_tsv(self):
        path = tempfile.mktemp()
        with open(path, 'w') as handle:
            write_tsv(handle, 'A', 'B', 'C')
        with open(path) as handle:
            self.assertEqual('A\tB\tC\n', handle.read())

    def test_iter_tsv(self):
        path = tempfile.mktemp()

        with open(path, 'w') as handle:
            write_tsv(handle, 'A', 'B', 'C')

        with open(path) as handle:
            row = next(iter_tsv(handle))
            self.assertEqual(3, len(row))

        with open(path) as handle:
            row = next(iter_tsv(handle, cols=('a', 'b', 'c')))
            self.assertEqual(3, len(row))
            self.assertEqual(row.a, 'A')
            self.assertEqual(row.b, 'B')
            self.assertEqual(row.c, 'C')

        with open(path) as handle:
            exception_raised = False
            try:
                row = next(iter_tsv(handle, cols=('a', 'b')))
            except TypeError:
                exception_raised = True
            self.assertTrue(exception_raised)

        with open(path) as handle:
            row = next(iter_tsv(handle, cols=('a', 0, 0)))
            self.assertEqual(3, len(row))
            self.assertEqual(row.a, 'A')
            self.assertFalse(hasattr(row, 'b'))
            self.assertFalse(hasattr(row, 'c'))

        with open(path) as handle:
            row = next(iter_tsv(handle, cols=('X', 'b', 'X')))
            self.assertEqual(3, len(row))
            self.assertEqual(row.b, 'B')
            self.assertFalse(hasattr(row, 'a'))
            self.assertFalse(hasattr(row, 'c'))

class TSVFormatTest(unittest.TestCase):

    def test_target(self):
        path = tempfile.mktemp()
        target = luigi.LocalTarget(path=path, format=TSV)
        with target.open('w') as handle:
            self.assertTrue(hasattr(handle, 'write_tsv'))
        with target.open() as handle:
            self.assertTrue(hasattr(handle, 'iter_tsv'))


if __name__ == '__main__':
    unittest.main()
