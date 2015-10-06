# coding: utf-8
#
#  Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                 by The Finc Authors, http://finc.info
#                 by Martin Czygan, <martin.czygan@uni-leipzig.de>
#
# This file is part of some open source application.
#
# Some open source application is free software: you can redistribute
# it and/or modify it under the terms of the GNU General Public
# License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.
#
# Some open source application is distributed in the hope that it will
# be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
#
# @license GPL-3.0+ <http://spdx.org/licenses/GPL-3.0+>
#

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
