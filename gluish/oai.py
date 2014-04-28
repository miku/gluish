# coding: utf-8

"""
OAI-PHM harvesting helper.
"""

from gluish.path import unlink
from gluish.utils import shellout, download, random_string
import BeautifulSoup
import logging
import os
import urllib

logger = logging.getLogger('gluish')


def oai_harvest(url=None, collection=None, begin=None, end=None,
                prefix='oai_dc', verb='ListRecords',
                max_retries=8, directory=None, ext='xml', download=download):
    """
    Harvest OAI for `url`. Will download all files into `directory`

    argument    OAI name
    --------------------
    begin       from
    collection  set
    end         until
    prefix      metadataPrefix
    verb        verb
    """
    if url is None:
        raise RuntimeError('A URL must be given.')
    if directory is None:
        raise RuntimeError('A directory must be given.')
    if not os.path.exists(directory):
        raise RuntimeError('Directory does not exist: %s' % directory)

    params = {'from': begin, 'until': end,
              'metadataPrefix': prefix, 'set': collection,
              'verb': verb}
    params = dict([(k, v) for k, v in params.iteritems() if v])

    # first request with all params
    full_url = '%s?%s' % (url, urllib.urlencode(params))
    path = os.path.join(directory, '%s.%s' % (random_string(length=16), ext))

    for retry in range(max_retries):
        try:
            download(url=full_url, filename=path, timeout=30)
            break
        except RuntimeError as err:
            logger.info('Retry %s on %s' % (retry, full_url))
            unlink(path)
    else:
        raise RuntimeError('Max retries (%s) exceeded: %s' % (
                           max_retries, full_url))

    # any subsequent request uses 'resumptiontoken'
    while True:
        with open(path) as handle:
            soup = BeautifulSoup.BeautifulStoneSoup(handle.read())
        token = soup.find('resumptiontoken')
        if token is None:
            break

        # subsequent requests are done with resumptiontoken only ...
        params = {'resumptionToken': token.text, 'verb': verb}
        full_url = '%s?%s' % (url, urllib.urlencode(params))
        path = os.path.join(directory, "%s.%s" % (
                            random_string(length=16), ext))

        retry = 0
        while True:
            if retry >= max_retries:
                raise RuntimeError("Max retries (%s) exceeded: %s" % (
                                   max_retries, full_url))
            try:
                download(url=full_url, filename=path)
                break
            except RuntimeError as err:
                retry += 1
                logger.info("Retry #%s on %s" % (retry, full_url))
                unlink(path)
