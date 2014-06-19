#!/usr/bin/env python
# coding: utf-8
# pylint: disable=C0301
"""
Parameter add-ons
=================

Custom luigi parameters.

"""
# pylint: disable=F0401,C0103,R0921,E1101,W0232,R0201,R0903,E1002
import luigi


class ZeroPaddedIntParameter(luigi.Parameter):
    """
    Parse an int, but pad. Result will be a string.
    """
    def __init__(self, *args, **kwargs):
        """ Use `pad` kwarg or 4. """
        self.pad = int(kwargs.pop('pad', 4))
        super(ZeroPaddedIntParameter, self).__init__(*args, **kwargs)

    def parse(self, s):
        """ Parse the value and pad left with zeroes. """
        _ = int(s)
        return str(s).zfill(self.pad)


class ILNParameter(luigi.Parameter):
    """
    Parse ILN (internal library number), so that the result is always in the
    *4 char* format.
    """
    def parse(self, s):
        """ Parse the value and pad left with zeroes. """
        return s.zfill(4)

class ClosestDateParameter(luigi.DateParameter):
    """ A marker parameter to replace date parameter value with whatever
    self.closest() returns. """
    use_closest_date = True


