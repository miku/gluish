Gluish
======

[![Build Status](https://travis-ci.org/miku/gluish.png)](https://travis-ci.org/miku/gluish)

Some glue around [luigi](https://github.com/spotify/luigi).

Provides a base class, that autogenerates its output filenames based on
* some base path,
* a tag,
* the classname and the
* significant parameters.

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
