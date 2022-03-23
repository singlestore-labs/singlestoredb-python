from __future__ import annotations

import json
from typing import Any
from typing import Dict

from mysql.connector.conversion import MySQLConverter

from .base import Driver


class Converter(MySQLConverter):

    def _BIT_to_python(self, value: Any, dsc: Any = None) -> Any:
        """Return bit value as-is."""
        return value

    def _JSON_to_python(self, value: Any, dsc: Any = None) -> Any:
        """Return Python object from JSON."""
        val = self._STRING_to_python(value, dsc)
        return json.loads(val)


class MySQLConnectorDriver(Driver):

    name = 'mysql.connector'

    pkg_name = 'mysql.connector'
    pypi = 'mysql-connector-python'
    anaconda = 'mysql-connector-python'

    def remap_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        params.pop('driver', None)
        params.pop('odbc_driver', None)
        if params.pop('pure_python', False):
            params['use_pure'] = True
        params['port'] = params['port'] or 3306
        params['allow_local_infile'] = params.pop('local_infile')
        params['raw'] = params.pop('raw_values')
        params['converter_class'] = Converter
        return params

    def is_connected(self, conn: Any, reconnect: bool = False) -> bool:
        try:
            conn.ping(reconnect=reconnect)
            return True
        except conn.InterfaceError:
            return False
