# coding: utf-8
# pylint: disable=C0103

"""
Test mixed utils.
"""

from gluish.utils import date_range, shellout
import datetime
import os
import sys
import tempfile
import unittest

class UtilsTest(unittest.TestCase):
    """ Test various utility functions. """
    def test_date_range(self):
        """ Test date ranges. """
        start_date = datetime.date(1970, 1, 1)
        end_date = datetime.date(1970, 10, 1)
        dates = [date for date in date_range(start_date, end_date, 2, 'months')]
        self.assertEquals(5, len(dates))

        start_date = datetime.date(1970, 1, 1)
        end_date = datetime.date(1970, 1, 3)
        dates = [date for date in date_range(start_date, end_date, 1, 'days')]
        self.assertEquals(3, len(dates))
        self.assertEquals(dates, [datetime.date(1970, 1, 1),
                                  datetime.date(1970, 1, 2),
                                  datetime.date(1970, 1, 3)])

    def test_shellout(self):
        """ Test external command calls. """
        output = shellout('ls 1> /dev/null && echo {output} 1> /dev/null',
                          output='hello')
        self.assertEquals('hello', output)

        path = tempfile.mktemp()
        with open(path, 'w') as handle:
            handle.write('Hello World!\n')

        output = shellout('wc -l < {input} > {output}', input=handle.name)
        self.assertTrue(os.path.exists(output))
        with open(output) as handle:
            self.assertEquals('1', handle.read().strip())

    @unittest.skipIf(sys.version_info.major > 2, 'skip on Python 3 for now.')
    def test_shellout_encoding(self):
        """ Test shellout encoding. """
        word = u'Catégorie'
        self.assertRaises(UnicodeEncodeError, shellout, 'echo {word}', word=word)

        output = shellout('echo {word} > {output}', word=word, encoding='utf-8')
        self.assertTrue(os.path.exists(output))
        with open(output) as handle:
            content = handle.read().strip()
            self.assertEquals('Cat\xc3\xa9gorie', content)
            self.assertEquals(u'Catégorie', content.decode('utf-8'))
