from __future__ import annotations

import json
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

    def after_connect(self, conn: Any, params: Dict[str, Any]) -> None:
        # This must be done afterward because use_unicode= whacks the
        # json converter if you try to put it in earlier.
        conn.converter[245] = json.loads

    def is_connected(self, conn: Any, reconnect: bool = False) -> bool:
        try:
            conn.ping(reconnect)
            return True
        except conn.InterfaceError:
            return False
