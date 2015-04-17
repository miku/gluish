# coding: utf-8

from gluish.benchmark import timed
from gluish.format import TSV
from gluish.utils import parse_isbns
import collections
import datetime
import luigi
import string
from elasticsearch import helpers as eshelpers
import elasticsearch


class ElasticsearchMixin(luigi.Task):
    """ A small mixin for tasks that require an ES connection. """
    es_host = luigi.Parameter(default='localhost', significant=False,
                              description='elasticsearch host')
    es_port = luigi.IntParameter(default=9200, significant=False,
                                 description='elasticsearch port')
    
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
            for _, hit in enumerate(hits):
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
            for _, hit in enumerate(hits):
                output.write_tsv(self.index, hit['_id'])

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class IndexFieldList(CommonTask):
    """
    Dump a list of field values from Elasticsearch as TSV.

    Example:

        $ taskdo IndexFieldList --index nep --doc-type title \
                                --fields "content.001 content.245.a 245.b" \
                                --null-value "NULL"
    """
    date = luigi.DateParameter(default=datetime.date.today())
    index = luigi.Parameter(description='index name or names separated by space')
    doc_type = luigi.Parameter(description='document type', default=None)
    fields = luigi.Parameter(description='the field(s) to dump')
    encoding = luigi.Parameter(default='utf-8')
    null_value = luigi.Parameter(default='<NULL>')

    raise_on_error = luigi.BooleanParameter(default=False,
                                            description='raise exception on missing values',
                                            significant=False)
    timeout = luigi.IntParameter(default=30, significant=False)
    size = luigi.IntParameter(default=50000, significant=False)
    scroll = luigi.Parameter(default='10m', significant=False)

    @timed
    def run(self):
        es = elasticsearch.Elasticsearch(timeout=self.timeout)
        indices = self.index.split()
        fields = self.fields.split()
        hits = eshelpers.scan(es, {'query': {'match_all': {}},
            'fields': fields}, index=indices, doc_type=self.doc_type,
            scroll=self.scroll, size=self.size)
        with self.output().open('w') as output:
            for hit in hits:
                hitfields = hit.get('fields')
                if not hitfields:
                    if self.raise_on_error:
                        raise RuntimeError("nothing found in document")
                    else:
                        continue
                else:
                    row = []
                    for field in fields:
                        value = hitfields.get(field, None)
                        if value is None:
                            row.append(self.null_value)
                        elif isinstance(value, basestring):
                            row.append(fieldvalue.encode(self.encoding))
                        elif isinstance(value, collections.Iterable):
                            row.append('|'.join([v.encode(self.encoding) for v in value]))
                        else:
                            if raise_on_error:
                                raise RuntimeError("unknown value type: {}".format(fields))
                    output.write_tsv(*row)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True), format=TSV)