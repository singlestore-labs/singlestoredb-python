from __future__ import annotations

import datetime
import json
from typing import Any
from typing import Dict
from typing import Optional

from .base import Driver


datetime_min = datetime.datetime.min
date_min = datetime.date.min
datetime_combine = datetime.datetime.combine


def convert_time(value: Optional[datetime.time]) -> Optional[datetime.timedelta]:
    """Convert time to timedelta."""
    if value is None:
        return None
    return datetime_combine(date_min, value) - datetime_min


def convert_json(value: Optional[str]) -> Optional[Dict[str, Any]]:
    """Convert JSON str to dict."""
    if value is None:
        return None
    return json.loads(value)


class PyODBCDriver(Driver):

    name = 'pyodbc'

    pkg_name = 'pyodbc'
    pypi = 'pyodbc'
    anaconda = 'pyodbc'

    def remap_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        out = {
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

        convs = params.pop('converters', {})
        self.converters = self.merge_converters(
            convs, {
                11: convert_time,
                245: convert_json,
            },
        )

        return out

    def is_connected(self, conn: Any, reconnect: bool = False) -> bool:
        return not conn.closed
