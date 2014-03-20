Gluish
======

[![Build Status](https://travis-ci.org/miku/gluish.png)](https://travis-ci.org/miku/gluish)

Some glue around [luigi](https://github.com/spotify/luigi).

Provides a base class, that autogenerates its output filenames based on some
tag, classname and parameters.

Additionally, provide some smaller utilities, like a TSV format, a benchmark
decorator and some task templates.


A `BaseTask` that knows its place
---------------------------------

You can use `gluish.task.BaseTask` as a superclass.

```python
from gluish.task import BaseTask
import datetime
import luigi
import tempfile

class DefaultTask(BaseTask):
    """ Some default abstract task for your tasks. """
    BASE = tempfile.gettempdir()
    TAG = 'just-a-test'

class RealTask(DefaultTask):
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