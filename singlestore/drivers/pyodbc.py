from __future__ import annotations

from typing import Any
from typing import Dict

from .base import Driver
# import json
# from ..converters import time_or_none


class PyODBCDriver(Driver):

    name = 'pyodbc'

    pkg_name = 'pyodbc'
    pypi = 'pyodbc'
    anaconda = 'pyodbc'

    def remap_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return {
            k: v for k, v in dict(
                server=params.get('host', '127.0.0.1'),
                port=params.get('port', 0) or 3306,
                database=params.get('database'),
                uid=params.get('user'),
                pwd=params.get('password'),
                charset=params.get('charset'),
                driver=params.get('odbc_driver'),
            ).items() if v is not None
        }

    def after_connect(self, conn: Any, params: Dict[str, Any]) -> None:
        if params.get('raw_values'):
            conn.clear_output_converters()
#       conn.add_output_converter(self.dbapi.SQL_TYPE_TIME, time_or_none)
#       conn.add_output_converter(245, json.loads)

    def is_connected(self, conn: Any, reconnect: bool = False) -> bool:
        return not conn.closed
