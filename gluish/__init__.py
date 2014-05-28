# coding: utf-8

"""
Gluish - some glue code and utils for luigi.

GLUISH_DATA: where common task place their artefacts
"""

import requests

requests.adapters.DEFAULT_RETRIES = 3
__version__ = '0.1.38'

# environment variable name
GLUISH_DATA = 'GLUISH_DATA'
