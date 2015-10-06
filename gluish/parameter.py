# coding: utf-8
# pylint: disable=F0401,C0103,R0921,E1101,W0232,R0201,R0903,E1002
#
#  Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                 by The Finc Authors, http://finc.info
#                 by Martin Czygan, <martin.czygan@uni-leipzig.de>
#
# This file is part of some open source application.
#
# Some open source application is free software: you can redistribute
# it and/or modify it under the terms of the GNU General Public
# License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.
#
# Some open source application is distributed in the hope that it will
# be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
#
# @license GPL-3.0+ <http://spdx.org/licenses/GPL-3.0+>
#

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
