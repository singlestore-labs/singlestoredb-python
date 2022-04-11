from __future__ import annotations

from json import loads as json_loads
from typing import Any
from typing import Dict
from typing import Optional

from .base import Driver


def convert_bit(value: Optional[int]) -> Optional[bytes]:
    if value is None:
        return None
    return value.to_bytes(8, byteorder='big')


def convert_json(value: Optional[str]) -> Optional[Dict[str, Any]]:
    if value is None:
        return None
    return json_loads(value)


def convert_set(value: Optional[set[str]]) -> Optional[str]:
    if value is None:
        return None
    if type(value) is set:
        return ','.join(value)
    return value  # type: ignore


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

        convs = params.pop('converters', {})
        self.converters = self.merge_converters(
            convs, {
                16: convert_bit,
                245: convert_json,
                253: convert_set,
            },
        )

        return params

    def is_connected(self, conn: Any, reconnect: bool = False) -> bool:
        try:
            conn.ping(reconnect=reconnect)
            return True
        except conn.InterfaceError:
            return False
