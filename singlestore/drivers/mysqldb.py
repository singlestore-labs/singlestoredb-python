from __future__ import annotations

from typing import Any
from typing import Dict

from ..converters import converters
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
        params.pop('credential_type', None)

        params['conv'] = dict(converters)

        params['ssl'] = {
            k: v for k, v in {
                'ssl_key': params.get('ssl_key', None),
                'ssl_cert': params.get('ssl_cert', None),
                'ssl_ca': params.get('ssl_ca', None),
            }.items()
        } or None

        if params.pop('ssl_disabled', False):
            params['ssl_mode'] = 'DISABLED'

        self.converters = params.pop('converters', {})

        return params

    def after_connect(self, conn: Any, params: Dict[str, Any]) -> None:
        # This must be done afterward because use_unicode= whacks the
        # json converter if you try to put it in earlier.
        conn.converter[245] = converters[245]

    def is_connected(self, conn: Any, reconnect: bool = False) -> bool:
        try:
            conn.ping(reconnect)
            return True
        except conn.InterfaceError:
            return False
