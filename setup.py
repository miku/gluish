# coding: utf-8

"""
Some glue around luigi.

Provides a base class, that autogenerates its output filenames based on some
tag, classname and parameters.

Additionally, provide some smaller utilities, like a TSV format, a benchmark
decorator and some task templates.
"""

try:
    from setuptools import setup
except:
    from distutils.core import setup


setup(name='gluish',
      version='0.1.1',
      description='Utils around Luigi.',
      url='https://github.com/miku/gluish',
      author='Martin Czygan',
      author_email='martin.czygan@gmail.com',
      packages=[
        'gluish',
      ],
      package_dir={'gluish': 'gluish'},
)
