# coding: utf-8
# pylint: disable=C0103
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

"""
Test mixed utils.
"""

import datetime
import os
import sys
import tempfile
import unittest

from gluish.utils import date_range, shellout


class UtilsTest(unittest.TestCase):
    """ Test various utility functions. """
    def test_date_range(self):
        """ Test date ranges. """
        start_date = datetime.date(1970, 1, 1)
        end_date = datetime.date(1970, 10, 1)
        dates = [date for date in date_range(start_date, end_date, 2, 'months')]
        self.assertEqual(5, len(dates))

        start_date = datetime.date(1970, 1, 1)
        end_date = datetime.date(1970, 1, 3)
        dates = [date for date in date_range(start_date, end_date, 1, 'days')]
        self.assertEqual(3, len(dates))
        self.assertEqual(dates, [datetime.date(1970, 1, 1),
                                  datetime.date(1970, 1, 2),
                                  datetime.date(1970, 1, 3)])

    def test_shellout(self):
        """ Test external command calls. """
        output = shellout('ls 1> /dev/null && echo {output} 1> /dev/null',
                          output='hello')
        self.assertEqual('hello', output)

        path = tempfile.mktemp()
        with open(path, 'w') as handle:
            handle.write('Hello World!\n')

        output = shellout('wc -l < {input} > {output}', input=handle.name)
        self.assertTrue(os.path.exists(output))
        with open(output) as handle:
            self.assertEqual('1', handle.read().strip())

    @unittest.skipIf(sys.version_info.major > 2, 'skip on Python 3 for now.')
    def test_shellout_encoding(self):
        """ Test shellout encoding. """
        word = u'Catégorie'
        self.assertRaises(UnicodeEncodeError, shellout, 'echo {word}', word=word)

        output = shellout('echo {word} > {output}', word=word, encoding='utf-8')
        self.assertTrue(os.path.exists(output))
        with open(output) as handle:
            content = handle.read().strip()
            self.assertEqual('Cat\xc3\xa9gorie', content)
            self.assertEqual(u'Catégorie', content.decode('utf-8'))
