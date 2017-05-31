# coding: utf-8
# pylint: disable=C0301
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
A few utilities.
"""

import logging
import os
import random
import re
import string
import subprocess
import tempfile

from dateutil import relativedelta

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

def which(program):
    """
    Search for program in PATH.
    """
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file

    return None

def shellout(template, preserve_whitespace=False, executable='/bin/bash',
             ignoremap=None, encoding=None, pipefail=True, **kwargs):
    """

    Takes a shell command template and executes it. The template must use the
    new (2.6+) format mini language. `kwargs` must contain any defined
    placeholder, only `output` is optional and will be autofilled with a
    temporary file if it used, but not specified explicitly.

    If `pipefail` is `False` no subshell environment will be spawned, where a
    failed pipe will cause an error as well. If `preserve_whitespace` is `True`,
    no whitespace normalization is performed. A custom shell executable name can
    be passed in `executable` and defaults to `/bin/bash`.

    Raises RuntimeError on nonzero exit codes. To ignore certain errors, pass a
    dictionary in `ignoremap`, with the error code to ignore as key and a string
    message as value.

    Simple template:

        wc -l < {input} > {output}

    Quoted curly braces:

        ps ax|awk '{{print $1}}' > {output}

    Usage with luigi:

        ...
        tmp = shellout('wc -l < {input} > {output}', input=self.input().path)
        luigi.LocalTarget(tmp).move(self.output().path)
        ....

    """
    if not 'output' in kwargs:
        kwargs.update({'output': tempfile.mkstemp(prefix='gluish-')[1]})
    if ignoremap is None:
        ignoremap = {}
    if encoding:
        command = template.decode(encoding).format(**kwargs)
    else:
        command = template.format(**kwargs)
    if not preserve_whitespace:
        command = re.sub('[ \t\n]+', ' ', command)
    if pipefail:
        command = '(set -o pipefail && %s)' % command
    logger.debug(command)
    code = subprocess.call([command], shell=True, executable=executable)
    if not code == 0:
        if code in ignoremap:
            logger.info("Ignoring error via ignoremap: %s" % ignoremap.get(code))
        else:
            logger.error('%s: %s' % (command, code))
            error = RuntimeError('%s exitcode: %s' % (command, code))
            error.code = code
            raise error
    return kwargs.get('output')
