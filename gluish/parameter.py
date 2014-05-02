#!/usr/bin/env python
# coding: utf-8
# pylint: disable=C0301
"""
Parameter add-ons
=================

Custom luigi parameters.

"""
# pylint: disable=F0401,C0103,R0921,E1101
from luigi.parameter import (MissingParameterException, _no_default,
                             ParameterException)
import abc
import datetime
import luigi


_no_default = object()


class ILNParameter(luigi.Parameter):
    """
    Parse ILN (internal library number), so that the result is always in the
    *4 char* format.
    """
    def parse(self, s):
        """ Parse the value and pad left with zeroes. """
        return s.zfill(4)

class ClosestDateParameter(luigi.DateParameter):
    use_closest_date = True


class AbstractMappedDateParameter(luigi.DateParameter):
    """ A date parameter, that maps certain values to others on the fly.
    Useful, if the data source has non-standard update cycles, that can be
    evaluated on runtime. """

    # non-atomically increasing counter used for ordering parameters.
    counter = 0

    def __init__(self, default=_no_default, is_list=False, is_boolean=False,
                 is_global=False, significant=True, description=None,
                 default_from_config=None):
        """
        :param default: the default value for this parameter. This should match the type of the
                        Parameter, i.e. ``datetime.date`` for ``DateParameter`` or ``int`` for
                        ``IntParameter``. You may only specify either ``default`` or
                        ``default_from_config`` and not both. By default, no default is stored and
                        the value must be specified at runtime.
        :param bool is_list: specify ``True`` if the parameter should allow a list of values rather
                             than a single value. Default: ``False``. A list has an implicit default
                             value of ``[]``.
        :param bool is_boolean: specify ``True`` if the parameter is a boolean value. Default:
                                ``False``. Boolean's have an implicit default value of ``False``.
        :param bool is_global: specify ``True`` if the parameter is global (i.e. used by multiple
                               Tasks). Default: ``False``.
        :param bool significant: specify ``False`` if the parameter should not be treated as part of
                                 the unique identifier for a Task. An insignificant Parameter might
                                 also be used to specify a password or other sensitive information
                                 that should not be made public via the scheduler. Default:
                                 ``True``.
        :param str description: A human-readable string describing the purpose of this Parameter.
                                For command-line invocations, this will be used as the `help` string
                                shown to users. Default: ``None``.
        :param dict default_from_config: a dictionary with entries ``section`` and ``name``
                                         specifying a config file entry from which to read the
                                         default value for this parameter. You may only specify
                                         either ``default`` or ``default_from_config`` and not both.
                                         Default: ``None``.
        """
        # The default default is no default
        self.__default = default  # We also use this to store global values
        self.is_list = is_list
        self.is_boolean = is_boolean and not is_list  # Only BooleanParameter should ever use this. TODO(erikbern): should we raise some kind of exception?
        self.is_global = is_global  # It just means that the default value is exposed and you can override it
        self.significant = significant # Whether different values for this parameter will differentiate otherwise equal tasks
        if is_global and default == _no_default and default_from_config is None:
            raise ParameterException('Global parameters need default values')
        self.description = description

        if default != _no_default and default_from_config is not None:
            raise ParameterException('Can only specify either a default or a default_from_config')
        if default_from_config is not None and (not 'section' in default_from_config or not 'name' in default_from_config):
            raise ParameterException('default_from_config must be a hash containing entries for section and name')
        self.default_from_config = default_from_config

        # We need to keep track of this to get the order right (see Task class)
        self.counter = AbstractMappedDateParameter.counter
        AbstractMappedDateParameter.counter += 1


    @property
    def default(self):
        """The default value for this Parameter.

        :raises MissingParameterException: if a default is not set.
        :return: the parsed default value.
        """
        if self.__default == _no_default and self.default_from_config is None:
            raise MissingParameterException("No default specified")
        if self.__default != _no_default:
            try:
                return self.mapper(self.__default)
            except NotImplementedError:
                return self.__default

        value = self._get_default_from_config(safe=False)
        if self.is_list:
            return tuple(self.parse(p.strip()) for p in value.strip().split('\n'))
        else:
            return self.parse(value)

    def set_default(self, value):
        """Set the default value of this Parameter.

        :param value: the new default value.
        """
        self.__default = value

    @property
    def has_default(self):
        """``True`` if a default was specified or if default_from_config references a valid entry in the conf."""
        if self.default_from_config is not None:
            return self._get_default_from_config(safe=True) is not None
        return self.__default != _no_default

    @abc.abstractmethod
    def mapper(self, date):
        """ Must return the mapped date (object) for a given date (object).

            task = SomeOtherTask(date=date)
            luigi.build([task], local_scheduler=True)
            s = task.output().open().read().strip()
            return datetime.date(*(int(v) for v in s.split('-')))
        """
        raise NotImplementedError

    def parse(self, s):
        """ Parse value into date and map it. """
        date = datetime.date(*(int(v) for v in s.split('-')))
        return self.mapper(date)
