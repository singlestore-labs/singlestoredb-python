#!/usr/bin/env python
"""SingleStoreDB package installer."""
import platform
from typing import Tuple

from setuptools import Extension
from setuptools import setup
from wheel.bdist_wheel import bdist_wheel


is_universal2 = (
    platform.platform().startswith('mac') and
    'x86_64' in platform.platform()
)


class bdist_wheel_abi3(bdist_wheel):

    def get_tag(self) -> Tuple[str, str, str]:
        python, abi, plat = super().get_tag()

        if python.startswith('cp'):
            if is_universal2:
                plat = plat.replace('x86_64', 'universal2')

            # on CPython, our wheels are abi3 and compatible back to 3.6
            return 'cp36', 'abi3', plat

        return python, abi, plat


setup(
    ext_modules=[
        Extension(
            '_pymysqlsv',
            sources=['singlestoredb/clients/pymysqlsv/accel.c'],
            define_macros=[('Py_LIMITED_API', '0x03060000')],
            # Set these using ARCHFLAGS='-arch x86_64 -arch arm64' with setup.py build
            extra_compile_args=['-arch', 'x86_64', '-arch', 'arm64'] \
            if is_universal2 else [],
            extra_link_args=['-arch', 'x86_64', '-arch', 'arm64'] \
            if is_universal2 else [],
            py_limited_api=True,
        ),
    ],
    cmdclass={'bdist_wheel': bdist_wheel_abi3},
)
