Gluish
======

[![Build Status](https://travis-ci.org/miku/gluish.png)](https://travis-ci.org/miku/gluish)

Some glue around [luigi](https://github.com/spotify/luigi).

Provides a base class, that autogenerates its output filenames based on
* some base path,
* a tag,
* the task id (the classname and the significant parameters)

Additionally, this package provides a few smaller utilities, like a TSV format,
a benchmarking decorator and some task templates.

A basic task that knows its place
---------------------------------

`gluish.task.BaseTask` is intended to be used as a supertask.

```python
from gluish.task import BaseTask
import datetime
import luigi
import tempfile

class DefaultTask(BaseTask):
    """ Some default abstract task for your tasks. BASE and TAG determine
    the paths, where the artefacts will be stored. """
    BASE = tempfile.gettempdir()
    TAG = 'just-a-test'

class RealTask(DefaultTask):
    """ Note that this task has a `self.path()`, that figures out the full
    path for this class' output. """
    date = luigi.DateParameter(default=datetime.date(1970, 1, 1))
    def run(self):
        with self.output().open('w') as output:
            output.write('Hello World!')

    def output(self):
        return luigi.LocalTarget(path=self.path())
```

When instantiating a `RealTask` instance, it will automatically be assigned a
structured output path, consisting of `BASE`, `TAG`, task name and a slugified
version of the significant parameters.

```python

task = RealTask()
task.output().path
# would be something like this on OS X:
# /var/folders/jy/g_b2kpwx0850/T/just-a-test/RealTask/date-1970-01-01.tsv

```

A TSV format
------------

Was started on the
[mailing list](https://groups.google.com/forum/#!searchin/luigi-user/TSV/luigi-user/F813st16xqw/xErC6pXR8zEJ).
Continuing  the example from above, lets create a task, that generates TSV
files, named `TabularSource`.

```python

from gluish.format import TSV

class TabularSource(DefaultTask):
    date = luigi.DateParameter(default=datetime.date(1970, 1, 1))
    def run(self):
        with self.output().open('w') as output:
            for i in range(10):
                output.write_tsv(i, 'Hello', 'World')

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
```

Another class, `TabularConsumer` can use `iter_tsv` on the handle obtained
by opening the file. The `row` will be a tuple, or - if `cols` is specified -
a `collections.namedtuple`.

```python
class TabularConsumer(DefaultTask):
    date = luigi.DateParameter(default=datetime.date(1970, 1, 1))
    def requires(self):
        return TabularSource()

    def run(self):
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('id', 'greeting', 'greetee'))
                print('Greeting: {}'.format(row.greeting))
                print('Greetee: {}'.format(row.greetee))

    def output(self):
        return luigi.LocalTarget(path=self.path())

```

A benchmark decorator
---------------------

Log some running times. Mostly useful in interactive mode.

```python
from gluish.benchmark import timed

class SomeWork(luigi.Task):

    @timed
    def run(self):
        pass

    def complete(self):
        return False
```

A task template for Elasticsearch
---------------------------------

Modeled after [luigi.contrib.CopyToTable](https://github.com/spotify/luigi/blob/01514d4559901ec62432cd13c48d9431b02433be/luigi/contrib/rdbms.py#L13).

```python
from gluish.esindex import CopyToIndex
import luigi

class ExampleIndex(CopyToIndex):
    host = 'localhost'
    port = 9200
    index = 'example'
    doc_type = 'default'
    purge_existing_index = True

    def docs(self):
        return [{'_id': 1, 'title': 'An example document.'}]

if __name__ == '__main__':
    task = ExampleIndex()
    luigi.build([task], local_scheduler=True)
```

Development
-----------

System package dependencies:

* Ubuntu: libmysqlclient-dev
* CentOS: mysql-devel

Setup:

    $ git clone git@github.com:miku/gluish.git
    $ cd gluish
    $ mkvirtualenv gluish
    $ pip install -r requirements.txt
    $ nosetests

Coverage status
---------------

As of 4a7ec26.

    $ nosetests --verbose --with-coverage --cover-package gluish
    Name               Stmts   Miss  Cover   Missing
    ------------------------------------------------
    gluish                 4      0   100%
    gluish.benchmark      32      3    91%   59-62
    gluish.colors         14      4    71%   12, 20, 24, 32
    gluish.common        103     18    83%   68, 72, 80-81, 83, 86, ...
    gluish.database       50     18    64%   12, 69-76, 79-88, 91-94
    gluish.esindex       105     71    32%   40-43, 62-66, 70-71, 77-86, ...
    gluish.format         27      2    93%   69, 72
    gluish.intervals      16      0   100%
    gluish.oai            58     14    76%   83-99
    gluish.parameter       5      0   100%
    gluish.path           82     14    83%   25, 39-48, 101-102, 165
    gluish.task           45      2    96%   95, 114
    gluish.utils          80      3    96%   35, 127, 166
    ------------------------------------------------
    TOTAL                621    149    76%
    ----------------------------------------------------------------------
    Ran 33 tests in 0.269s
