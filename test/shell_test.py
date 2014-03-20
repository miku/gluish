# coding: utf-8

from gluish.shell import shellout
from gluish.path import touch
import unittest
import tempfile
import os


class ShellTest(unittest.TestCase):

    def test_shellout(self):
        output = shellout('ls 1> /dev/null && echo {output} 1> /dev/null',
                          output='hello')
        self.assertEquals('hello', output)

        path = tempfile.mktemp()
        with open(path, 'w') as handle:
            handle.write('Hello World!\n')

        output = shellout('wc -l < {input} > {output}', input=handle.name)
        self.assertTrue(os.path.exists(output))
        with open(output) as handle:
            self.assertEquals('1', handle.read().strip())
