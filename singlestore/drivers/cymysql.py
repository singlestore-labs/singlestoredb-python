from __future__ import annotations

import json
from typing import Any
from typing import Dict

try:
    from cymysql.converters import decoders
except ImportError:
    decoders = {}

from .base import Driver


decoders = dict(decoders)
decoders[245] = json.loads


class CyMySQLDriver(Driver):

    name = 'CyMySQL'

    pkg_name = 'cymysql'
    pypi = 'cymysql'
    anaconda = 'cymysql'

    def remap_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        params.pop('driver', None)
        params.pop('pure_python', None)
        params.pop('odbc_driver', None)
        params.pop('local_infile', None)
        params['port'] = params['port'] or 3306
        params['db'] = params.pop('database', None)
        params['passwd'] = params.pop('password', None)
        params['conv'] = decoders
        if params['raw_values']:
            params['conv'] = {}
        params.pop('raw_values', None)
        return params

    def is_connected(self, conn: Any, reconnect: bool = False) -> bool:
        return conn.ping(reconnect=reconnect)
