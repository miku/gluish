# coding: utf-8

"""
OAI harvesting tests.
"""

# pylint: disable=F0401,W0613
from gluish.oai import oai_harvest
import unittest
import tempfile
import random

def download_first_request(url=None, filename=None, **kwargs):
    """ Initial download. """
    with open(filename, 'w') as output:
        output.write('some successful first response XML')

def download_second_request(url=None, filename=None, **kwargs):
    """ Download XML with resumptiontoken. """
    with open(filename, 'w') as output:
        output.write('some successful second response XML')

def download_failed(url=None, filename=None, **kwargs):
    """ A failed download. """
    raise RuntimeError()

def download_flaky(probability=1.0):
    """ Fail to download with probability `probability` """
    def download(*args, **kwargs):
        """ Raise RuntimeError with probability `probability`. """
        if random.random() < probability:
            raise RuntimeError()
        else:
            download_first_request(*args, **kwargs)
    return download


class OAITest(unittest.TestCase):
    """ Test harvesting. """

    def test_oai_harvest(self):
        """ Test harvesting. """
        exception_raised = False
        try:
            oai_harvest(url='http://nirvana', directory=tempfile.gettempdir(),
                        download=download_failed)
        except RuntimeError:
            exception_raised = True
        self.assertTrue(exception_raised)

        # probability of this test failing: 7.888609052210118e-29.
        oai_harvest(url='http://nirvana', directory=tempfile.gettempdir(),
                    max_retries=100, download=download_flaky(probability=0.5))
