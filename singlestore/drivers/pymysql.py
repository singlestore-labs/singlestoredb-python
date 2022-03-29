from __future__ import annotations

import json
from typing import Any
from typing import Dict

try:
    from pymysql.converters import conversions
except ImportError:
    conversions = {}

from .base import Driver


conversions = dict(conversions)
conversions[245] = json.loads


class PyMySQLDriver(Driver):

    name = 'PyMySQL'

    pkg_name = 'pymysql'
    pypi = 'pymysql'
    anaconda = 'pymysql'

    def remap_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        params.pop('driver', None)
        params.pop('odbc_driver', None)
        params.pop('pure_python', None)
        params['port'] = params['port'] or 3306
        params['conv'] = conversions
        if params['raw_values']:
            params['conv'] = {}
        params.pop('raw_values', None)
        return params

    def is_connected(self, conn: Any, reconnect: bool = False) -> bool:
        return conn.open
