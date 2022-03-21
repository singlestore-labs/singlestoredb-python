from __future__ import annotations

from typing import Any
from typing import Dict

from .base import Driver


class MySQLConnectorDriver(Driver):

    name = 'mysql.connector'

    pkg_name = 'mysql.connector'
    pypi = 'mysql-connector-python'
    anaconda = 'mysql-connector-python'

    def remap_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        params.pop('driver', None)
        params.pop('odbc_driver', None)
        params['use_pure'] = params.pop('pure_python', False)
        params['port'] = params['port'] or 3306
        params['allow_local_infile'] = params.pop('local_infile')
        return params
