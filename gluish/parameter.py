#!/usr/bin/env python
# coding: utf-8

"""
Parameter add-ons
=================

Custom luigi parameters.

"""

from luigi.parameter import MissingParameterException, _no_default
import abc
import datetime
import luigi


class ILNParameter(luigi.Parameter):
    """
    Parse ILN (internal library number), so that the result is always in the
    *4 char* format.
    """
    def parse(self, s):
        return s.zfill(4)


class AbstractMappedDateParameter(luigi.Parameter):
    """ A date parameter, that maps certain values to others on the fly.
    Useful, if the data source has non-standard update cycles, that can be
    evaluated on runtime. """

    @property
    def default(self):
        """The default value for this Parameter.

        :raises MissingParameterException: if a default is not set.
        :return: the parsed default value.
        """
        if self.__default == _no_default and self.default_from_config is None:
            raise MissingParameterException("No default specified")
        if self.__default != _no_default:
            return self.mapped_date(self.__default)

        value = self._get_default_from_config(safe=False)
        if self.is_list:
            return tuple(self.parse(p.strip()) for p in value.strip().split('\n'))
        else:
            return self.parse(value)

    @abc.abstractmethod
    def mapped_date(self, date):
        """ Return the mapped date (object) for a date (object).
        
        Example implementations:

        # using another task
        task = SomeOtherTask(date=date)
        luigi.build([task], local_scheduler=True)
        return task.output().open().read().strip()
        """

    def parse(self, s):
        date = datetime.date(*(int(v) for v in s.split('-')))
        return self.mapped_date(date)
