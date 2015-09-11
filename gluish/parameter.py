# coding: utf-8
# pylint: disable=F0401,C0103,R0921,E1101,W0232,R0201,R0903,E1002

"""
Custom luigi parameters.
"""

import luigi

__all__ = ['ClosestDateParameter']

class ClosestDateParameter(luigi.DateParameter):
    """
    A marker parameter to replace date parameter value with whatever
    self.closest() returns. Use in conjunction with `gluish.task.BaseTask`.
    """
    use_closest_date = True
