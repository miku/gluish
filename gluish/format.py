#!/usr/bin/env python
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

"""
Format add-ons
==============

Format related functions and classes. Highlights: A TSV class, that helps
to work with tabular data.

Example:

    def run(self):
        with self.output().open('w') as output:
            output.write_tsv('Ubik', '1969', '67871286')

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

"""

from __future__ import unicode_literals

import collections
import functools
from builtins import str

import luigi
from gluish.utils import random_string, which
from six import string_types

__all__ = ['TSV']


def write_tsv(output_stream, *tup, **kwargs):
    """
    Write argument list in `tup` out as a tab-separeated row to the stream.
    """
    encoding = kwargs.get('encoding') or 'utf-8'
    value = u'\t'.join([s for s in tup]) + '\n'
    if encoding is None:
        if isinstance(value, string_types):
            output_stream.write(value.encode('utf-8'))
        else:
            output_stream.write(value)
    else:
        output_stream.write(value.encode(encoding))


def iter_tsv(input_stream, cols=None, encoding='utf-8'):
    """
    If a tuple is given in cols, use the elements as names to construct
    a namedtuple.

    Columns can be marked as ignored by using ``X`` or ``0`` as column name.

    Example (ignore the first four columns of a five column TSV):

    ::

        def run(self):
            with self.input().open() as handle:
                for row in handle.iter_tsv(cols=('X', 'X', 'X', 'X', 'iln')):
                    print(row.iln)
    """
    if cols:
        cols = [c if not c in ('x', 'X', 0, None) else random_string(length=5)
                for c in cols]
        Record = collections.namedtuple('Record', cols)
        for line in input_stream:
            yield Record._make(line.decode(encoding).rstrip('\n').split('\t'))
    else:
        for line in input_stream:
            yield tuple(line.decode(encoding).rstrip('\n').split('\t'))


class TSVFormat(luigi.format.Format):
    """
    A basic CSV/TSV format.
    Discussion: https://groups.google.com/forum/#!topic/luigi-user/F813st16xqw
    """

    def hdfs_reader(self, input_pipe):
        raise NotImplementedError()

    def hdfs_writer(self, output_pipe):
        raise NotImplementedError()

    def pipe_reader(self, input_pipe):
        input_pipe.iter_tsv = functools.partial(iter_tsv, input_pipe)
        return input_pipe

    def pipe_writer(self, output_pipe):
        output_pipe.write_tsv = functools.partial(write_tsv, output_pipe)
        return output_pipe


class GzipFormat(luigi.format.Format):
    """
    A gzip format, that upgrades itself to pigz, if it's installed.
    """
    input = 'bytes'
    output = 'bytes'

    def __init__(self, compression_level=None):
        self.compression_level = compression_level
        self.gzip = ["gzip"]
        self.gunzip = ["gunzip"]

        if which('pigz'):
            self.gzip = ["pigz"]
            self.gunzip = ["unpigz"]

    def pipe_reader(self, input_pipe):
        return luigi.format.InputPipeProcessWrapper(self.gunzip, input_pipe)

    def pipe_writer(self, output_pipe):
        args = self.gzip
        if self.compression_level is not None:
            args.append('-' + str(int(self.compression_level)))
        return luigi.format.OutputPipeProcessWrapper(args, output_pipe)

class ZstdFormat(luigi.format.Format):
    """
    The zstandard format.
    """
    input = 'bytes'
    output = 'bytes'

    def __init__(self, compression_level=None):
        self.compression_level = compression_level
        self.zstd = ["zstd"]
        self.unzstd = ["unzstd"]

    def pipe_reader(self, input_pipe):
        return luigi.format.InputPipeProcessWrapper(self.unzstd, input_pipe)

    def pipe_writer(self, output_pipe):
        args = self.zstd
        if self.compression_level is not None:
            args.append('-' + str(int(self.compression_level)))
        return luigi.format.OutputPipeProcessWrapper(args, output_pipe)

TSV = TSVFormat()
Gzip = GzipFormat()
Zstd = ZstdFormat()
