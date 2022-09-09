from __future__ import annotations

from typing import Any
from typing import Callable
from typing import Dict

try:
    from pymysql.converters import encoders
except ImportError:
    encoders = {}

from .base import Driver
from ..converters import converters as convs

converters: Dict[Any, Callable[..., Any]] = dict(convs)
converters.update(encoders)


class PyMySQLDriver(Driver):

    name = 'PyMySQL'

    pkg_name = 'pymysql'
    pypi = 'pymysql'
    anaconda = 'pymysql'

    def remap_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        params.pop('driver', None)
        params.pop('odbc_driver', None)
        params.pop('pure_python', None)
        params.pop('credential_type', None)
        # The cipher must be set to this level for SingleStoreDB Cloud.
        params['ssl'] = dict(cipher='HIGH')
        params['port'] = params['port'] or 3306
        params['conv'] = self.merge_converters(params.pop('converters', {}), converters)
        return params

    def is_connected(self, conn: Any, reconnect: bool = False) -> bool:
        return conn.open
