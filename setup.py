#!/usr/bin/env python
"""SingleStoreDB package installer."""
import os
import platform
from typing import Tuple

from setuptools import Extension
from setuptools import setup
from wheel.bdist_wheel import bdist_wheel


py_limited_api = '0x03080000'
# py_limited_api = False

build_extension = bool(int(os.environ.get('SINGLESTOREDB_BUILD_EXTENSION', '1')))

universal2_flags = ['-arch', 'x86_64', '-arch', 'arm64'] \
    if (
        platform.platform().startswith('mac') and
        'x86_64' in platform.platform()
) else []


class bdist_wheel_abi3(bdist_wheel):

    def get_tag(self) -> Tuple[str, str, str]:
        python, abi, plat = super().get_tag()

        if python.startswith('cp'):
            if universal2_flags:
                plat = plat.replace('x86_64', 'universal2')

            # on CPython, our wheels are abi3 and compatible back to 3.8
            return 'cp38', 'abi3', plat

        return python, abi, plat


if build_extension:
    setup(
        ext_modules=[
            Extension(
                '_singlestoredb_accel',
                sources=['accel.c'],
                define_macros=[
                    ('Py_LIMITED_API', py_limited_api),
                ] if py_limited_api else [],
                py_limited_api=bool(py_limited_api),
                extra_compile_args=universal2_flags,
                extra_link_args=universal2_flags,
            ),
        ],
        cmdclass={'bdist_wheel': bdist_wheel_abi3 if py_limited_api else bdist_wheel},
    )
else:
    setup()
