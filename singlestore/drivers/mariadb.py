from __future__ import annotations

from typing import Any
from typing import Dict

from .base import Driver


class MariaDBDriver(Driver):

    name = 'mariadb'

    pkg_name = 'mariadb'
    pypi = 'mariadb-connector-python'
    anaconda = 'mariadb-connector-python'

    def remap_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        params.pop('driver', None)
        params.pop('charset', None)
        params.pop('odbc_driver', None)
        params.pop('pure_python', None)
        return params
