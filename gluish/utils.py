# coding: utf-8
# pylint: disable=C0301

"""
A few utilities.
"""

from dateutil import relativedelta
import logging
import random
import re
import string
import subprocess
import tempfile

__all__ = [
    'date_range',
    'random_string',
    'shellout',
]

logger = logging.getLogger('gluish')

def random_string(length=16):
    """
    Return a random string (upper and lowercase letters) of length `length`,
    defaults to 16.
    """
    return ''.join(random.choice(string.ascii_letters) for _ in range(length))

def date_range(start_date, end_date, increment, period):
    """
    Generate `date` objects between `start_date` and `end_date` in `increment`
    `period` intervals.
    """
    next = start_date
    delta = relativedelta.relativedelta(**{period:increment})
    while next <= end_date:
        yield next
        next += delta

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
        tmp = shellout('wc -l < {input} > {output}', input=self.input().path)
        luigi.File(tmp).move(self.output().path)
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
    logger.debug(command)
    code = subprocess.call([command], shell=True)
    if not code == 0:
        if code in ignoremap:
            logger.info("Ignoring error via ignoremap: %s" % ignoremap.get(code))
        else:
            logger.error('%s: %s' % (command, code))
            error = RuntimeError('%s exitcode: %s' % (command, code))
            error.code = code
            raise error
    return kwargs.get('output')
