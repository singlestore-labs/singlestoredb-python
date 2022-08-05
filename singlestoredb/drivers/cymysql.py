from __future__ import annotations

from typing import Any
from typing import Dict

try:
    from cymysql.converters import encoders
except ImportError:
    encoders = {}

from .base import Driver
from ..converters import converters


converters = dict(converters)
converters.update(encoders)


class CyMySQLDriver(Driver):

    name = 'CyMySQL'

    pkg_name = 'cymysql'
    pypi = 'cymysql'
    anaconda = 'cymysql'

    def remap_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        params.pop('driver', None)
        params.pop('pure_python', None)
        params.pop('odbc_driver', None)
        params.pop('credential_type', None)
        params['port'] = params['port'] or 3306
        params['db'] = params.pop('database', None)
        params['passwd'] = params.pop('password', None)
        params['conv'] = self.merge_converters(params.pop('converters', {}), converters)
        return params

    def is_connected(self, conn: Any, reconnect: bool = False) -> bool:
        return conn.ping(reconnect=reconnect)
