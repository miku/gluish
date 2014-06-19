# coding: utf-8

from gluish.parameter import ZeroPaddedIntParameter
import luigi
import unittest


class ParameterTest(unittest.TestCase):
    """ Test parameters. """
    def test_zero_padded_int_parameter(self):
        """ Test padding. """
        parameter = ZeroPaddedIntParameter(pad=7)
        self.assertEquals(parameter.parse(12), '0000012')

        parameter = ZeroPaddedIntParameter()
        self.assertEquals(parameter.parse(12), '0012')

        parameter = ZeroPaddedIntParameter(pad=-1)
        self.assertEquals(parameter.parse(12), '12')

        parameter = ZeroPaddedIntParameter(pad=1)
        self.assertEquals(parameter.parse(12), '12')

        parameter = ZeroPaddedIntParameter()
        with self.assertRaises(ValueError):
            parameter.parse("Hello")
