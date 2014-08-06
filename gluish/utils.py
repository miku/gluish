# coding: utf-8

"""
Various utilities.
"""

from dateutil import relativedelta
from gluish.colors import cyan
import collections
import cPickle
import functools
import itertools
import logging
import operator
import pyisbn
import random
import re
import string
import subprocess
import tempfile
import time


logger = logging.getLogger('gluish')


class DefaultOrderedDict(collections.OrderedDict):
    """ Cross-over of ordered and default dict. """
    def __init__(self, default_factory=None, *a, **kw):
        if (default_factory is not None and
            not isinstance(default_factory, collections.Callable)):
            raise TypeError('first argument must be callable')
        collections.OrderedDict.__init__(self, *a, **kw)
        self.default_factory = default_factory

    def __getitem__(self, key):
        try:
            return collections.OrderedDict.__getitem__(self, key)
        except KeyError:
            return self.__missing__(key)

    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        self[key] = value = self.default_factory()
        return value

    def __reduce__(self):
        if self.default_factory is None:
            args = tuple()
        else:
            args = self.default_factory,
        return type(self), args, None, None, self.items()

    def copy(self):
        return self.__copy__()

    def __copy__(self):
        return type(self)(self.default_factory, self)

    def __deepcopy__(self, memo):
        import copy
        return type(self)(self.default_factory,
                          copy.deepcopy(self.items()))
    def __repr__(self):
        return 'OrderedDefaultDict(%s, %s)' % (self.default_factory,
            collections.OrderedDict.__repr__(self))


class DotDict(dict):
    """ Access dictionary values via dot notation. """
    __getattr__ = dict.__getitem__

    def __init__(self, dictionary):
        self.update(**dict((k, self.parse(v))
                           for k, v in dictionary.iteritems()))

    @classmethod
    def parse(cls, value):
        """ Parse the dict value. """
        if isinstance(value, dict):
            return cls(value)
        elif isinstance(value, list):
            return [cls.parse(i) for i in value]
        else:
            return value


def flatten(iterable):
    """
    Flattens arbitrary nested lists into a flat list. """
    for item in iterable:
        if (isinstance(item, collections.Iterable) and
            not isinstance(item, basestring)):
            for sub in flatten(item):
                yield sub
        else:
            yield item

def itervalues(iterable):
    """
    Flattens arbitrary nested lists and dict values into a flat list. """
    for item in iterable:
        if isinstance(item, collections.Mapping):
            for k, v in item.iteritems():
                if isinstance(v, collections.Iterable) or isinstance(v, collections.Mapping):
                    for vv in itervalues(v):
                        yield vv
                else:
                    yield v
        elif (isinstance(item, collections.Iterable) and
            not isinstance(item, basestring)):
            for sub in flatten(item):
                yield sub
        else:
            yield item

def pairwise(obj):
    """ Iterator over a iterable in steps of two. """
    iterable = iter(obj)
    return itertools.izip(iterable, iterable)


def nwise(iterable, n=2):
    """
    Generalized :func:`pairwise`.
    Split an iterable after every `n` items.
    """
    i = iter(iterable)
    piece = tuple(itertools.islice(i, n))
    while piece:
        yield piece
        piece = tuple(itertools.islice(i, n))


def download(url=None, filename=None, timeout=60):
    """ Download a URL content to filename. """
    shellout("""wget --timeout {timeout} -q --retry-connrefused
                -O {output} '{url}' """, url=url, output=filename,
                timeout=timeout)


def date_range(start_date, end_date, increment, period):
    """
    Generate `date` objects between `start_date` and `end_date` in `increment`
    `period` intervals.
    """
    result = []
    nxt = start_date
    delta = relativedelta.relativedelta(**{period:increment})
    while nxt <= end_date:
        result.append(nxt)
        nxt += delta
    return result


def normalize(s):
    """
    Quick and dirty string normalizer.
    """
    return ' '.join(re.sub('[^a-zA-Z0-9 ]', '', s).lower().split())


def random_string(length=16):
    """
    Return a random string (upper and lowercase letters) of length `length`,
    defaults to 16.
    """
    return ''.join(random.choice(string.letters) for _ in range(length))


def dashify(s):
    """
    Convert CamelCase to camel-case,
    http://stackoverflow.com/a/1176023/89391.
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1-\2', s)
    result = re.sub('([a-z0-9])([A-Z])', r'\1-\2', s1).lower()
    return istrip(result)


def istrip(s):
    """
    Remove all spaces and newlines from a string.
    """
    return re.sub(r'\s+', '', s.strip().replace('\n', ''))


def unwrap(s):
    """
    Remove newlines and repeated spaces from a string.
    """
    if not isinstance(s, basestring):
        return s
    s = re.sub(r'\s+', ' ', s.strip().replace('\n', ' '))
    return s


def shellout(template, **kwargs):
    """
    Takes a shell command template and executes it. The template must use
    the new (2.6+) format mini language. `kwargs` must contain any defined
    placeholder, only `output` is optional.
    Raises RuntimeError on nonzero exit codes.

    Simple template:

        wc -l < {input} > {output}

    Quoted curly braces:

        ps ax|awk '{{print $1}}' > {output}

    Usage with luigi:

        ...
        tmp = shellout('wc -l < {input} > {output}', input=self.input().fn)
        luigi.File(tmp).move(self.output.fn())
        ....

    """
    preserve_whitespace = kwargs.get('preserve_whitespace', False)
    if not 'output' in kwargs:
        kwargs.update({'output': tempfile.mkstemp(prefix='gluish-')[1]})
    ignoremap = kwargs.get('ignoremap', {})
    encoding = kwargs.get('encoding', None)
    if encoding:
        command = template.decode(encoding).format(**kwargs)
    else:
        command = template.format(**kwargs)
    if not preserve_whitespace:
        command = re.sub('[ \t\n]+', ' ', command)
    logger.debug(cyan(command))
    code = subprocess.call([command], shell=True)
    if not code == 0:
        if code in ignoremap:
            logger.info("Ignoring error via ignoremap: %s" % (
                        ignoremap.get(code)))
        else:
            logger.error('%s: %s' % (command, code))
            error = RuntimeError('%s exitcode: %s' % (command, code))
            error.code = code
            raise error
    return kwargs.get('output')


def parse_isbns(s):
    """ Given a string, find as many uniq ISBNs in it an return them. """
    pattern = re.compile('[0-9X-]{10,25}')
    isbns = set()
    for candidate in pattern.findall(s):
        candidate = candidate.replace('-', '').replace(' ', '')
        if len(candidate) == 10:
            try:
                isbns.add(pyisbn.convert(candidate))
            except pyisbn.IsbnError as err:
                logger.error('%s: %s' % (s, err))
        elif len(candidate) == 13:
            isbns.add(candidate)
    return list(isbns)


class memoize(object):
    '''Decorator. Caches a function's return value each time it is called.
    If called later with the same arguments, the cached value is returned
    (not reevaluated).
    '''
    def __init__(self, func):
        self.func = func
        self.cache = {}
    def __call__(self, *args):
        if not isinstance(args, collections.Hashable):
            # uncacheable. a list, for instance.
            # better to not cache than blow up.
            return self.func(*args)
        if args in self.cache:
            return self.cache[args]
        else:
            value = self.func(*args)
            self.cache[args] = value
            return value
    def __repr__(self):
        '''Return the function's docstring.'''
        return self.func.__doc__
    def __get__(self, obj, objtype):
        '''Support instance methods.'''
        return functools.partial(self.__call__, obj)


class cached_property(object):
    '''Decorator for read-only properties evaluated only once within TTL period.

    It can be used to created a cached property like this::

        import random

        # the class containing the property must be a new-style class
        class MyClass(object):
            # create property whose value is cached for ten minutes
            @cached_property(ttl=600)
            def randint(self):
                # will only be evaluated every 10 min. at maximum.
                return random.randint(0, 100)

    The value is cached  in the '_cache' attribute of the object instance that
    has the property getter method wrapped by this decorator. The '_cache'
    attribute value is a dictionary which has a key for every property of the
    object which is wrapped by this decorator. Each entry in the cache is
    created only when the property is accessed for the first time and is a
    two-element tuple with the last computed property value and the last time
    it was updated in seconds since the epoch.

    The default time-to-live (TTL) is 300 seconds (5 minutes). Set the TTL to
    zero for the cached value to never expire.

    To expire a cached property value manually just do::

        del instance._cache[<property name>]

    '''
    def __init__(self, ttl=300):
        self.ttl = ttl

    def __call__(self, fget, doc=None):
        self.fget = fget
        self.__doc__ = doc or fget.__doc__
        self.__name__ = fget.__name__
        self.__module__ = fget.__module__
        return self

    def __get__(self, inst, owner):
        now = time.time()
        try:
            value, last_update = inst._cache[self.__name__]
            if self.ttl > 0 and now - last_update > self.ttl:
                raise AttributeError
        except (KeyError, AttributeError):
            value = self.fget(inst)
            try:
                cache = inst._cache
            except AttributeError:
                cache = inst._cache = {}
            cache[self.__name__] = (value, now)
        return value
