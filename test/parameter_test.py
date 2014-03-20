# coding: utf-8

from gluish.parameter import ILNParameter
import unittest


class ParameterTest(unittest.TestCase):

    def test_iln_parameter(self):
        p = ILNParameter()
        self.assertEquals('0010', p.parse('10'))
