from __future__ import annotations

from typing import Any
from typing import Dict

from .base import Driver


class MySQLdbDriver(Driver):

    name = 'MySQLdb'

    pkg_name = 'MySQLdb'
    pypi = 'mysqlclient'
    anaconda = 'mysqlclient'

    def remap_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        params.pop('driver', None)
        params.pop('pure_python', None)
        params.pop('odbc_driver', None)
        if params['raw_values']:
            params['conv'] = {}
        params.pop('raw_values', None)
        return params
