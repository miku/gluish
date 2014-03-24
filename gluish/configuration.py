# coding: utf-8

"""
Configuration handling, adapted from luigi.
"""

from ConfigParser import ConfigParser, NoOptionError, NoSectionError
import datetime


class BaseConfigParser(ConfigParser):
    """ A ConfigParser, that can be queried for typed configuration entries. """
    NO_DEFAULT = None
    _instance = None
    _config_paths = []

    @classmethod
    def add_config_path(cls, path):
        """ Append a path to read from. """
        cls._config_paths.append(path)
        cls.instance().reload()

    @classmethod
    def instance(cls, *args, **kwargs):
        """ Singleton getter """
        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
            _ = cls._instance.reload()

        return cls._instance

    def reload(self):
        """ Reload the configuration. """
        return self._instance.read(self._config_paths)

    def _get_with_default(self, method, section, option, default,
                          expected_type=None):
        """
        Gets the value of the section/option using method. Returns default
        if value is not found. Raises an exception if the default value
        is not None and doesn't match the expected_type.
        """
        try:
            return method(self, section, option)
        except (NoOptionError, NoSectionError):
            if default is BaseConfigParser.NO_DEFAULT:
                raise
            if (expected_type is not None and default is not None and
                not isinstance(default, expected_type)):
                raise
            return default

    def get(self, section, option, default=NO_DEFAULT):
        """ Default. """
        return self._get_with_default(ConfigParser.get, section, option,
                                      default)

    def getboolean(self, section, option, default=NO_DEFAULT):
        """ Return a boolean. """
        return self._get_with_default(ConfigParser.getboolean, section, option,
                                      default, bool)

    def getint(self, section, option, default=NO_DEFAULT):
        """ Return an int. """
        return self._get_with_default(ConfigParser.getint, section, option,
                                      default, int)

    def getfloat(self, section, option, default=NO_DEFAULT):
        """ Return a float. """
        return self._get_with_default(ConfigParser.getfloat, section, option,
                                      default, float)

    def getdate(self, section, option, default=NO_DEFAULT, fmt='%Y-%m-%d'):
        """ Return a datetime.date instance. """
        value = self.get(section, option, default=default)
        return datetime.datetime.strptime(value, fmt).date()

    def getdatetime(self, section, option, default=NO_DEFAULT,
                    fmt='%Y-%m-%d'):
        """ Return a datetime. """
        value = self.get(section, option, default=default)
        return datetime.datetime.strptime(value, fmt)


def get_config():
    """ Convenience method (for backwards compatibility) for accessing
        config singleton """
    return BaseConfigParser.instance()

