# coding: utf-8

"""
A wrapper around subprocess.call, that handles parameters, inputs and outputs.
"""

from gluish.colors import cyan
import logging
import re
import subprocess
import tempfile

logger = logging.getLogger('gluish')


def shellout(template, **kwargs):
    """
    Takes a shell command template and executes it. The template must use
    the new (2.6+) format mini language. `kwargs` must contain any defined
    placeholder, only `output` is optional.
    Raises RuntimeError on nonzero exit codes.

    Simple template:

        wc -l < {input} > {output}

    Quoted curly braces:

        ps ax|awk '{{print $1}}' > {output}

    Usage with luigi:

        ...
        tmp = shellout('wc -l < {input} > {output}', input=self.input().fn)
        luigi.File(tmp).move(self.output.fn())
        ....

    """
    preserve_spaces = kwargs.get('preserve_spaces', False)
    stopover = tempfile.mktemp(prefix='gluish')
    if not 'output' in kwargs:
        kwargs.update({'output': stopover})
    ignoremap = kwargs.get('ignoremap', {})
    command = template.format(**kwargs)
    if not preserve_spaces:
        command = re.sub('[ \n]+', ' ', command)
    logger.debug(cyan(command))
    code = subprocess.call([command], shell=True)
    if not code == 0:
        if code in ignoremap:
            logger.info("Ignoring error via ignoremap: %s" % (ignoremap.get(code)))
        else:
            logger.error('%s: %s' % (command, code))
            error = RuntimeError('%s exitcode: %s' % (command, code))
            error.code = code
            raise error
    return kwargs.get('output')
