# coding: utf-8

"""
Gluish - some glue code and utils for luigi.

GLUISH_DATA: where common task place their artefacts
"""

import requests

requests.adapters.DEFAULT_RETRIES = 3
__version__ = '0.1.71'

# environment variable name
GLUISH_DATA = 'GLUISH_DATA'


import gluish.benchmark
import gluish.colors
import gluish.common
import gluish.configuration
import gluish.database
import gluish.esindex
import gluish.format
import gluish.intervals
import gluish.oai
import gluish.parameter
import gluish.path
import gluish.task
import gluish.utils
