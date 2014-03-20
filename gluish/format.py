#!/usr/bin/env python
# coding: utf-8

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

from gluish.utils import random_string
import collections
import functools
import luigi


def write_tsv(output_stream, *tup):
    """
    Write argument list in `tup` out as a tab-separeated row to the stream.
    """
    output_stream.write('\t'.join([str(s) for s in tup]) + '\n')
    # output_stream.write('\t'.join([s.encode('utf-8') for s in tup]) + '\n')


def iter_tsv(input_stream, cols=None):
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
            yield Record._make(line.rstrip('\n').split('\t'))
    else:
        for line in input_stream:
            yield tuple(line.rstrip('\n').split('\t'))


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

TSV = TSVFormat()
