# coding: utf-8

from gluish.oai import oai_harvest
import unittest
import tempfile
import random

def download_first_request(url=None, filename=None):
    with open(filename, 'w') as output:
        output.write('some successful first response XML')

def download_second_request(url=None, filename=None):
    with open(filename, 'w') as output:
        output.write('some successful second response XML')

def download_failed(url=None, filename=None):
    raise RuntimeError()

def download_flaky(p=1.0):
    """ Fail to download with probability `p` """    
    def download(*args, **kwargs):
        if random.random() < p:
            raise RuntimeError()
        else:
            download_first_request(*args, **kwargs)
    return download        


class OAITest(unittest.TestCase):

    def test_oai_harvest(self):
        with self.assertRaises(RuntimeError):
            oai_harvest(url='http://nirvana', directory=tempfile.gettempdir(),
                        download=download_failed)

        # probability of this test failing: 7.888609052210118e-29.
        oai_harvest(url='http://nirvana', directory=tempfile.gettempdir(),
                    max_retries=100, download=download_flaky(p=0.5))
