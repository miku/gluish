# coding: utf-8

"""
Some glue around luigi.

Provides a base class, that autogenerates its output filenames based on some
tag, classname and parameters.

Additionally, provide some smaller utilities, like a TSV format.
"""

from setuptools import setup

install_requires = [
    'luigi>=1.0.20',
    'python-dateutil==2.2',
    'pytz==2014.4',
    'six==1.9.0',
]

setup(name='gluish',
      version='0.2.0',
      description='Luigi helper.',
      url='https://github.com/miku/gluish',
      author='Martin Czygan',
      author_email='martin.czygan@gmail.com',
      packages=[
        'gluish',
      ],
      package_dir={'gluish': 'gluish'},
      install_requires=install_requires,
)
