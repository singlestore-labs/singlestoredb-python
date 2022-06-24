from __future__ import annotations

import json
from typing import Any
from typing import Dict

from .base import Driver


converters = {245: json.loads}


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
        params.pop('credential_type', None)
        params['converter'] = self.merge_converters(
            params.pop('converters', {}),
            dict(converters),
        )
        if params.pop('ssl_disabled', False):
            params['ssl'] = False
            params['ssl_verify_cert'] = False

        return params

    def is_connected(self, conn: Any, reconnect: bool = False) -> bool:
        try:
            conn.ping()
            return True
        except conn.InterfaceError:
            return False
