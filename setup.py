#!/usr/bin/env python
"""SingleStoreDB package installer."""
from typing import Tuple

from setuptools import Extension
from setuptools import setup
from wheel.bdist_wheel import bdist_wheel


class bdist_wheel_abi3(bdist_wheel):

    def get_tag(self) -> Tuple[str, str, str]:
        python, abi, plat = super().get_tag()

        if python.startswith('cp'):
            # on CPython, our wheels are abi3 and compatible back to 3.6
            return 'cp36', 'abi3', plat

        return python, abi, plat


setup(
    ext_modules=[
        Extension(
            '_pymysqlsv',
            sources=['singlestoredb/clients/pymysqlsv/accel.c'],
            define_macros=[('Py_LIMITED_API', '0x03060000')],
            py_limited_api=True,
        ),
    ],
    cmdclass={'bdist_wheel': bdist_wheel_abi3},
)
