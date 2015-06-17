# coding: utf-8

"""
Some glue around luigi.

Provides a base class, that autogenerates its output filenames based on some
tag, classname and parameters.

Additionally, provide some smaller utilities, like a TSV format, a benchmark
decorator and some task templates.
"""
from setuptools import setup
import sys

install_requires = [
    'astroid>=1.0.1',
    'BeautifulSoup==3.2.1',
    'colorama==0.3.3',
    'elasticsearch==1.3.0',
    'logilab-common==0.61.0',
    'luigi>=1.0.20',
    'nose==1.3.3',
    'pyisbn==1.0.0',
    'PyMySQL==0.6.6',
    'python-dateutil==2.2',
    'pytz==2014.4',
    'requests==2.5.1',
    'six==1.9.0',
    'urllib3==1.10',
    'wsgiref==0.1.2',
]

if sys.version_info < (2, 7):
    install_requires.append('ordereddict==1.1')

setup(name='gluish',
      version='0.1.78',
      description='Utils around Luigi.',
      url='https://github.com/miku/gluish',
      author='Martin Czygan',
      author_email='martin.czygan@gmail.com',
      packages=[
        'gluish',
      ],
      package_dir={'gluish': 'gluish'},
      install_requires=install_requires,
      extras_require={'sqlitebck': ['sqlitebck==1.2.1']}
)
