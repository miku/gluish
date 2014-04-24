# coding: utf-8

"""
Test mixed utils.
"""

from gluish.utils import (flatten, pairwise, nwise, DotDict, date_range,
                          normalize, random_string, dashify, unwrap, istrip,
                          shellout)
import datetime
import os
import tempfile
import unittest

class UtilsTest(unittest.TestCase):
    """ Test various utility functions. """

    def test_flatten(self):
        """ Test flattening of nested list. """
        self.assertEquals([1, 2, 3], list(flatten([1, [2, [3]]])))
        self.assertEquals([1, 2, 3], list(flatten([1, [2, 3]])))
        self.assertEquals([1], list(flatten({1: [2, 3]})))

    def test_pairwise(self):
        """ Test take 2. """
        self.assertEquals([], list(pairwise(range(1))))
        self.assertEquals([(0, 1), (2, 3)], list(pairwise(range(4))))
        self.assertEquals([(0, 1), (2, 3)], list(pairwise(range(5))))
        self.assertEquals([(0, 1), (2, 3), (4, 5)], list(pairwise(range(6))))

    def test_nwise(self):
        """ Test take n. """
        self.assertEquals([(0,), (1,), (2,), (3,)], list(nwise(range(4), n=1)))
        self.assertEquals([(0, 1), (2, 3)], list(nwise(range(4))))
        self.assertEquals([(0, 1, 2), (3,)], list(nwise(range(4), n=3)))
        self.assertEquals([(0, 1, 2, 3)], list(nwise(range(4), n=4)))

    def test_date_range(self):
        """ Test date ranges. """
        start_date = datetime.date(1970, 1, 1)
        end_date = datetime.date(1970, 10, 1)
        dates = date_range(start_date, end_date, 2, 'months')
        self.assertEquals(5, len(dates))

        start_date = datetime.date(1970, 1, 1)
        end_date = datetime.date(1970, 1, 3)
        dates = date_range(start_date, end_date, 1, 'days')
        self.assertEquals(3, len(dates))
        self.assertEquals(dates, [datetime.date(1970, 1, 1),
                                  datetime.date(1970, 1, 2),
                                  datetime.date(1970, 1, 3)])


    def test_normalize(self):
        s = "Hello, World!"
        self.assertEquals("hello world", normalize(s))

        s = "Hello, World 123&&&!"
        self.assertEquals("hello world 123", normalize(s))

    def test_random_string(self):
        """ Test random string length. """
        self.assertEquals(16, len(random_string()))
        self.assertEquals(10, len(random_string(length=10)))

    def test_dashify(self):
        """ Test dashify. """
        self.assertEquals('camel-case', dashify('CamelCase'))
        self.assertEquals('ibm-no-no-no', dashify('IBMNoNoNo'))
        self.assertEquals('code123-red', dashify('Code123Red'))
        self.assertEquals('yes-or-no', dashify('yes-or-no'))
        self.assertEquals('even-spaces', dashify('Even Spaces'))

    def test_istrip(self):
        """ Test inner strip. """
        self.assertEquals('yes', istrip('y es'))
        self.assertEquals('yes', istrip('y e    s'))
        self.assertEquals('yesorno', istrip('y e    s\nor no'))

    def test_unwrap(self):
        """ Test unwrapping. """
        self.assertEquals('hello world', unwrap('hello    world'))
        self.assertEquals('hello world, how are you',
                          unwrap('hello    world,\n how   are\n you'))

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


class DotDictTest(unittest.TestCase):
    """ Test dictionary with dot access. """
    def test_dot_dict(self):
        dd = DotDict({'a': 1, 'b': 2, 'c': {'d': 3}, 'e': {'f': {'g': 4}}})
        self.assertEquals(1, dd.a)
        self.assertEquals(2, dd.b)
        self.assertEquals({'d': 3}, dd.c)
        self.assertEquals(3, dd.c.d)
        self.assertEquals(4, dd.e.f.g)
