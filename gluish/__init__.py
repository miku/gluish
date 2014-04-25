# coding: utf-8

"""
Gluish - some glue code and utils for luigi.
"""

import requests

requests.adapters.DEFAULT_RETRIES = 3
__version__ = '0.1.13'
