#!/usr/bin/env python
"""SingleStoreDB package installer."""
import os
import platform
import re
import sys
import sysconfig
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import setuptools
from setuptools import Extension
from setuptools import setup
from setuptools.command.bdist_wheel import bdist_wheel
from setuptools.command.build_ext import build_ext


# Free-threaded CPython cannot load abi3 extensions, so it gets one built for
# the free-threaded stable ABI, abi3t (PEP 803), which starts at 3.15. The
# wheel is tagged abi3t alone, not abi3.abi3t, so GIL-enabled interpreters
# keep installing the cp38-abi3 wheel. 3.13t and 3.14t have no stable ABI;
# a source build there uses the full C API.
free_threaded = bool(sysconfig.get_config_var('Py_GIL_DISABLED'))

stable_abi_macro: Optional[Tuple[str, str]]
if not free_threaded:
    stable_abi_macro = ('Py_LIMITED_API', '0x03080000')
    wheel_tag = ('cp38', 'abi3')
elif sys.version_info >= (3, 15):
    stable_abi_macro = ('Py_TARGET_ABI3T', '0x030F0000')
    wheel_tag = ('cp315', 'abi3t')
else:
    stable_abi_macro = None
# stable_abi_macro = None  # full C API everywhere, for debugging

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

            # on CPython, our wheels target a stable ABI: abi3 back to 3.8,
            # or abi3t from 3.15
            return wheel_tag[0], wheel_tag[1], plat

        return python, abi, plat


class build_ext_abi3t(build_ext):

    def get_export_symbols(self, ext: Extension) -> List[str]:
        # setuptools always adds PyInit_<name>, which an abi3t module does not
        # define: it has PyModExport_<name> instead (PEP 793). Only Windows
        # uses this list, and its linker fails on the missing PyInit_.
        return [f'PyModExport_{ext.name.split(".")[-1]}']


def setuptools_version() -> Tuple[int, ...]:
    return tuple(int(x) for x in re.findall(r'\d+', setuptools.__version__)[:2])


if (
    build_extension and stable_abi_macro
    and stable_abi_macro[0] == 'Py_TARGET_ABI3T'
    and setuptools_version() < (84, 2)
):
    # Older setuptools names the module .abi3.so, which free-threaded
    # interpreters do not load. pyproject.toml asks for 84.2 on 3.15+.
    raise RuntimeError(
        f'building for abi3t needs setuptools>=84.2, not {setuptools.__version__}',
    )


if build_extension:
    cmdclass: Dict[str, type] = {
        'bdist_wheel': bdist_wheel_abi3 if stable_abi_macro else bdist_wheel,
    }
    if stable_abi_macro and stable_abi_macro[0] == 'Py_TARGET_ABI3T':
        cmdclass['build_ext'] = build_ext_abi3t

    setup(
        ext_modules=[
            Extension(
                '_singlestoredb_accel',
                sources=['accel.c'],
                define_macros=[stable_abi_macro] if stable_abi_macro else [],
                py_limited_api=bool(stable_abi_macro),
                extra_compile_args=universal2_flags,
                extra_link_args=universal2_flags,
            ),
        ],
        cmdclass=cmdclass,
    )
else:
    setup()
