# coding: utf-8

"""
Tasks that can (mostly) be used out of the box.
"""
# pylint: disable=F0401,W0232,R0903,E1101
from elasticsearch import helpers as eshelpers
from gluish import GLUISH_DATA
from gluish.benchmark import timed
from gluish.format import TSV
from gluish.oai import oai_harvest
from gluish.path import iterfiles, which
from gluish.task import BaseTask
from gluish.utils import shellout, random_string, parse_isbns
import BeautifulSoup
import collections
import datetime
import elasticsearch
import hashlib
import json
import logging
import luigi
import os
import pipes
import re
import requests
import string
import tempfile


logger = logging.getLogger('gluish')


class CommonTask(BaseTask):
    """
    A base class for common classes. These artefacts will be written to the
    systems tempdir.
    """
    BASE = os.environ.get(GLUISH_DATA, tempfile.gettempdir())
    TAG = 'common'


class Indices(CommonTask):
    """
    List all ES indices and doc counts.
    """
    host = luigi.Parameter(default='localhost')
    port = luigi.IntParameter(default=9200)

    @timed
    def run(self):
        """ Write info about indices to stdout. """
        es = elasticsearch.Elasticsearch([{'host': self.host,
                                           'port': self.port}])
        stats = es.indices.stats()
        indices = collections.Counter()
        for key, value in stats.get('indices').iteritems():
            indices[key] = value.get('primaries').get('docs').get('count')
        total = sum(indices.values())
        print(json.dumps(dict(indices=indices, total=total,
                              sources=len(indices)), indent=4))

    def complete(self):
        return False


class SplitFile(CommonTask):
    """
    Idempotent wrapper around split -l.
    Given a filename and the number of chunks, the output of this task is
    a single file, which contains the paths to the chunk files, one per line.
    """
    filename = luigi.Parameter()
    chunks = luigi.IntParameter(default=1)

    def run(self):
        line_count = sum(1 for line in open(self.filename))
        lines = int((line_count + self.chunks) / self.chunks)

        taskdir = os.path.dirname(self.output().fn)
        if not os.path.exists(taskdir):
            os.makedirs(taskdir)

        prefix = random_string()
        shellout("cd {taskdir} && split -l {lines} {input} {prefix}",
                 taskdir=taskdir, lines=lines, input=self.filename,
                 prefix=prefix)

        with self.output().open('w') as output:
            for path in sorted(iterfiles(taskdir)):
                if os.path.basename(path).startswith(prefix):
                    output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True), format=TSV)


class Executable(CommonTask):
    """
    Checks, whether an external executable is available. This task will consider
    itself complete, only if the executable `name` is found in PATH on the
    system.
    """
    name = luigi.Parameter()
    message = luigi.Parameter(default="")

    def run(self):
        """ Only run if, task is not complete. """
        raise RuntimeError('External app %s required.\n%s' % (self.name,
                           self.message))

    def complete(self):
        return which(self.name) is not None


class LineCount(CommonTask):
    """ Wrapped wc -l. """
    def requires(self):
        raise NotImplementedError("Should be some file with lines to count.")

    @timed
    def run(self):
        """ wc -l wrapped. """
        tmp = shellout("wc -l < {input} > {output}", input=self.input().fn)
        luigi.File(tmp).move(self.output().fn)

    def output(self):
        raise NotImplementedError()


class OAIHarvestChunk(CommonTask):
    """ Template task to harvest a piece of OAI. """

    begin = luigi.DateParameter(default=datetime.date.today())
    end = luigi.DateParameter(default=datetime.date.today())
    prefix = luigi.Parameter(default="marc21")
    url = luigi.Parameter(default="http://oai.bnf.fr/oai2/OAIHandler")
    collection = luigi.Parameter(default=None)

    def run(self):
        stopover = tempfile.mkdtemp(prefix='gluish-')
        oai_harvest(url=self.url, begin=self.begin, end=self.end,
                    prefix=self.prefix, directory=stopover,
                    collection=self.collection)

        with self.output().open('w') as output:
            output.write("""<collection
                xmlns="http://www.openarchives.org/OAI/2.0/"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            """)
            for path in iterfiles(stopover):
                with open(path) as handle:
                    soup = BeautifulSoup.BeautifulStoneSoup(handle.read())
                    for record in soup.findAll('record'):
                        output.write(str(record)) # or unicode?
            output.write('</collection>\n')

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml', digest=True))


class FTPMirror(CommonTask):
    """
    A generic FTP directory sync. Outsource to lftp.
    """
    host = luigi.Parameter()
    username = luigi.Parameter(default='anonymous')
    password = luigi.Parameter(default='')
    pattern = luigi.Parameter(default='*', description="e.g. '*leip_*.zip'")
    base = luigi.Parameter(default='.')
    indicator = luigi.Parameter(default=random_string())

    def requires(self):
        return Executable(name='lftp')

    def run(self):
        """ The indicator is always recreated, while the subdir
        for a given (host, username, base, pattern) is just synced. """
        base = os.path.dirname(self.output().path)
        subdir = hashlib.sha1('{host}:{username}:{base}:{pattern}'.format(
            host=self.host, username=self.username, base=self.base,
            pattern=self.pattern)).hexdigest()
        # target is the root of the mirror
        target = os.path.join(base, subdir)
        if not os.path.exists(target):
            os.makedirs(target)

        command = """lftp -u {username},{password}
        -e "set net:max-retries 5; set net:timeout 10; mirror --verbose=0
        --only-newer -I {pattern} {base} {target}; exit" {host}"""

        shellout(command, host=self.host, username=pipes.quote(self.username),
                 password=pipes.quote(self.password),
                 pattern=pipes.quote(self.pattern),
                 target=pipes.quote(target),
                 base=pipes.quote(self.base))

        with self.output().open('w') as output:
            for path in iterfiles(target):
                logger.debug("Mirrored: %s" % path)
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True), format=TSV)


class FTPFile(CommonTask):
    """ Just require a single file from an FTP server. """
    host = luigi.Parameter()
    username = luigi.Parameter()
    password = luigi.Parameter()
    filepath = luigi.Parameter()

    def requires(self):
        return Executable(name='lftp')

    def run(self):
        command = """lftp -u {username},{password}
        -e "set net:max-retries 5; set net:timeout 10; get -c
        {filepath} -o {output}; exit" {host}"""

        output = shellout(command, host=self.host,
                          username=pipes.quote(self.username),
                          password=pipes.quote(self.password),
                          filepath=pipes.quote(self.filepath))
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True), format=TSV)


class Directory(luigi.Task):
    """ Create directory or fail. """
    path = luigi.Parameter(description='directory to create')

    def run(self):
        try:
            os.makedirs(self.path)
        except OSError as err:
            if err.errno == 17:
                # file exists, this can happen in parallel execution evns
                pass
            else:
                raise RuntimeError(err)

    def output(self):
        return luigi.LocalTarget(self.path)


class FXRates(CommonTask):
    """
    Download and parse XML EUR daily FX rates into TSV (CURRENCY, RATE)
    http://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml

    Example output:

        USD     1.3515
        JPY     131.76
        BGN     1.9558
        CZK     25.606
        DKK     7.4586
        GBP     0.83410
        HUF     296.70
        LTL     3.4528
        LVL     0.7027
        PLN     4.2190
        RON     4.4500
        ...
    """
    date = luigi.DateParameter(default=datetime.date.today())
    url = luigi.Parameter(
        default='http://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml')

    def run(self):
        r = requests.get(self.url)
        with self.output().open('w') as output:
            soup = BeautifulSoup.BeautifulStoneSoup(r.text)
            for el in soup.findAll('cube', currency=re.compile('.*')):
                output.write_tsv(el['currency'], el['rate'])

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True), format=TSV)


class IndexIsbnList(CommonTask):
    """ Get a list of ISBNs 13 for an index and a date. Uses the default
    layout with "content.020.a", "content.020.9", "content.020.z" and
    "content.776.z" fields. """
    date = luigi.DateParameter(default=datetime.date.today())
    index = luigi.Parameter(description='index name')
    size = luigi.IntParameter(default=50000, significant=False)
    scroll = luigi.Parameter(default='10m', significant=False)
    keys = luigi.Parameter(default='content.020.a,content.020.9,content.020.z,content.776.z')

    @timed
    def run(self):
        """ Get all fields, then do a basic sanity check via parse_isbns. """
        es = elasticsearch.Elasticsearch()
        isbn_fields = map(string.strip, self.keys.split(','))
        hits = eshelpers.scan(es, {'query': {'match_all': {}},
                                   'fields': isbn_fields}, index=self.index,
                              scroll=self.scroll, size=self.size)

        with self.output().open('w') as output:
            for i, hit in enumerate(hits):
                fields = hit.get('fields', {})
                isbns = set()
                for isbn_field in isbn_fields:
                    tag = isbn_field.replace("content.", "")
                    for value in fields.get(isbn_field, []):
                        if isinstance(value, basestring):
                            value = value.strip().replace('-', '')
                            if len(value) == 0:
                                continue
                            for isbn in parse_isbns(value):
                                isbns.add((tag, isbn.encode('utf-8')))
                        elif isinstance(value, list):
                            for v in value:
                                v = value.strip().replace('-', '')
                                if len(v) == 0:
                                    continue
                                for isbn in parse_isbns(value):
                                    isbns.add((tag, isbn.encode('utf-8')))
                for tag, isbn in isbns:
                    output.write_tsv(hit['_id'], isbn, tag)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True), format=TSV)


class IndexIdList(CommonTask):
    """ Dump a list of (index, _id) for a given index. """
    date = luigi.DateParameter(default=datetime.date.today())
    index = luigi.Parameter(description='index name')
    size = luigi.IntParameter(default=50000, significant=False)
    scroll = luigi.Parameter(default='10m', significant=False)

    @timed
    def run(self):
        """ Assumes local elasticsearch for now. """
        es = elasticsearch.Elasticsearch()
        hits = eshelpers.scan(es, {'query': {'match_all': {}}, 'fields': []},
                              index=self.index, scroll=self.scroll,
                              size=self.size)

        with self.output().open('w') as output:
            for i, hit in enumerate(hits):
                output.write_tsv(self.index, hit['_id'])

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
