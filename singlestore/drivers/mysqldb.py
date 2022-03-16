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
        return params
