#!/usr/bin/env python
"""SingleStoreDB package installer."""
from setuptools import Extension
from setuptools import setup

setup(
    ext_modules=[
        Extension(
            '_pymysqlsv',
            ['singlestoredb/clients/pymysqlsv/accel.c'],
            py_limited_api=True,
        ),
    ],
)
