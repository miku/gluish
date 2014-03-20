#!/usr/bin/env python
# coding: utf-8

"""
Parameter add-ons
=================

Custom luigi parameters.

"""

import luigi

class ILNParameter(luigi.Parameter):
    """ Parse ILN, so that the result is always in the "4 char" format. """
    def parse(self, s):
        return s.zfill(4)
