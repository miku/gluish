# coding: utf-8

"""
Configuration handling, adapted from luigi.
"""

from ConfigParser import ConfigParser, NoOptionError, NoSectionError
import datetime
import socket


class BaseConfigParser(ConfigParser):
    NO_DEFAULT = None
    _instance = None
    _config_paths = []

    @classmethod
    def add_config_path(cls, path):
        cls._config_paths.append(path)
        cls.instance().reload()

    @classmethod
    def instance(cls, *args, **kwargs):
        """ Singleton getter """
        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
            loaded = cls._instance.reload()

        return cls._instance

    def reload(self):
        return self._instance.read(self._config_paths)

    def _get_with_default(self, method, section, option, default, expected_type=None):
        """ Gets the value of the section/option using method. Returns default if value
        is not found. Raises an exception if the default value is not None and doesn't match
        the expected_type.
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
        return self._get_with_default(ConfigParser.get, section, option, default)

    def getboolean(self, section, option, default=NO_DEFAULT):
        return self._get_with_default(ConfigParser.getboolean, section, option, default, bool)

    def getint(self, section, option, default=NO_DEFAULT):
        return self._get_with_default(ConfigParser.getint, section, option, default, int)

    def getfloat(self, section, option, default=NO_DEFAULT):
        return self._get_with_default(ConfigParser.getfloat, section, option, default, float)

    def getdate(self, section, option, default=NO_DEFAULT):
        value = self.get(section, option, default=default)
        return datetime.datetime.strptime(value, self.get('core', 'date-format', '%Y-%m-%d')).date()


def get_config():
    """ Convenience method (for backwards compatibility) for accessing config singleton """
    return BaseConfigParser.instance()

