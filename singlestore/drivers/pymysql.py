from __future__ import annotations

from typing import Any
from typing import Dict

from .base import Driver


class PyMySQLDriver(Driver):

    name = 'PyMySQL'

    pkg_name = 'pymysql'
    pypi = 'pymysql'
    anaconda = 'pymysql'

    def remap_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        params.pop('driver', None)
        params.pop('pure_python', None)
        params['port'] = params['port'] or 3306
        return params
