#!/usr/bin/env python
from setuptools import setup

from singlestore import __version__

setup(
    name='singlestore',
    version=__version__,
    author='SingleStore',
    author_email='support@singlestore.com',
    url='http://github.com/singlestore-labs/singlestore-python',
    license=open('LICENSE', 'r').read(),
    description='',
    long_description=open('README.md', 'r').read(),
    long_description_content_type='text/markdown',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    packages=[
        'singlestore',
    ],
    install_requires=[
        'mysql-connector-python',
        'requests',
        'sqlparams',
    ],
    extras_require={
        'dataframe': ['pandas'],
    },
)
