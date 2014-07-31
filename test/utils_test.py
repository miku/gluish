# coding: utf-8

"""
Test mixed utils.
"""

# pylint: disable=C0103
from gluish.utils import (flatten, pairwise, nwise, DotDict, date_range,
                          normalize, random_string, dashify, unwrap, istrip,
                          shellout, parse_isbns, memoize, DefaultOrderedDict,
                          itervalues)
import collections
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
        """ Test simple string normalize. """
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

    def test_shellout_encoding(self):
        """ Test shellout encoding. """
        word = u'Catégorie'
        with self.assertRaises(UnicodeEncodeError):
            shellout('echo {word}', word=word)

        output = shellout('echo {word} > {output}', word=word, encoding='utf-8')
        self.assertTrue(os.path.exists(output))
        with open(output) as handle:
            content = handle.read().strip()
            self.assertEquals('Cat\xc3\xa9gorie', content)
            self.assertEquals(u'Catégorie', content.decode('utf-8'))

    def test_parse_isbns(self):
        """ Test ISBN parse. """
        self.assertEquals([], parse_isbns("Nothing"))
        self.assertEquals([], parse_isbns("123 Nothing"))
        self.assertEquals(['9780321349606'], parse_isbns("0-321-34960-1 Nothing"))
        self.assertEquals(['9780321349606'], parse_isbns("0-321-34960-1 0-321-34960-1 Nothing"))
        self.assertEquals(['9780321349606'], parse_isbns("0-321-34960-2 Nothing"))
        self.assertEquals(['9780321349613'], parse_isbns("0-321-34961-2 Nothing"))
        self.assertEquals(['9780321349606', '9780321349613'], parse_isbns("9780321349606 Nothing 0-321-34961-2"))
        self.assertEquals([], parse_isbns('8085800XXX'))

    def test_memoize(self):
        """ Test memoize """
        counter = collections.Counter()

        def f1(x):
            counter['f1'] += 1
            return 2 * x

        @memoize
        def f2(x):
            counter['f2'] += 1
            return 2 * x

        self.assertEquals(0, counter['f1'])
        self.assertEquals(4, f1(2))
        self.assertEquals(1, counter['f1'])
        self.assertEquals(4, f1(2))
        self.assertEquals(2, counter['f1'])

        self.assertEquals(0, counter['f2'])
        self.assertEquals(4, f2(2))
        self.assertEquals(1, counter['f2'])
        self.assertEquals(4, f2(2))
        self.assertEquals(1, counter['f2'])

    def test_itervalues(self):
        """ Test itervalues. """
        self.assertEquals([], list(itervalues([])))
        self.assertEquals([1, 2, 3], list(itervalues([1, 2, 3])))
        self.assertEquals([1, 2, 3, 4], list(itervalues([1, 2, [3, 4]])))
        self.assertEquals([1, 2, 3, 4], list(itervalues([1, 2, [3, 4]])))
        self.assertEquals([1, 2, 3, 4], list(itervalues([1, 2, [3, [4]]])))
        self.assertEquals([1, 2, 4], list(itervalues([1, 2, {3: 4}])))
        self.assertEquals([1, 2, 4, 5], list(itervalues([1, 2, {3: [4, 5]}])))
        self.assertEquals([1, 2, 4, 6], list(itervalues([1, 2, {3: [4, {5: 6}]}])))
        self.assertEquals([1, 2, 4, 6, 9], list(itervalues([1, 2, {3: [4, {5: 6}],
                                                                   7: [{8: 9}]}])))
        self.assertEquals([1, 2, "X"], list(itervalues([1, 2, {"_": "X"}])))
        self.assertEquals([1, 2, "Z", "X"], list(itervalues([1, 2, {"_": "X", 1: "Z"}])))


class DotDictTest(unittest.TestCase):
    """ Test dictionary with dot access. """
    def test_dot_dict(self):
        """ Test dot dict. """
        dd = DotDict({'a': 1, 'b': 2, 'c': {'d': 3}, 'e': {'f': {'g': 4}}})
        self.assertEquals(1, dd.a)
        self.assertEquals(2, dd.b)
        self.assertEquals({'d': 3}, dd.c)
        self.assertEquals(3, dd.c.d)
        self.assertEquals(4, dd.e.f.g)


class DefaultOrderedDictTest(unittest.TestCase):
    """ Test DefaultOrderedDict. """
    def test_default_ordered_dict(self):
        """ Test ordered dict with defaults. """
        d = DefaultOrderedDict(list)
        d[0].append(0)
        d[1].append(2)
        d[2].append(4)
        d[3].append(6)
        d[4].append(8)
        for i in range(5):
            self.assertEquals(d[i], [i * 2])
        keys = [k for k, _ in d.iteritems()]
        self.assertEquals(range(5), keys)
