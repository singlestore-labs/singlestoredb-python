from __future__ import annotations

from typing import Any
from typing import Dict

from .. import auth
from ..converters import converters
from .base import Driver


class MySQLConnectorDriver(Driver):

    name = 'mysqlconnector'

    pkg_name = 'mysql.connector'
    pypi = 'mysql-connector-python'
    anaconda = 'mysql-connector-python'

    # This flag lets the connection do the decoding of text / binary accordingly
    returns_bytes = True

    def remap_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        params.pop('driver', None)
        params.pop('odbc_driver', None)
        if params.pop('pure_python', False):
            params['use_pure'] = True
        params['port'] = params['port'] or 3306
        params['allow_local_infile'] = params.pop('local_infile')

        # Always use raw, we're doing the conversions ourselves
        params['raw'] = True

        convs = params.pop('converters', {})
        self.converters = self.merge_converters(convs, converters)

        # Check authentication method
        cred = params.pop('credential_type', None)
        if cred in [auth.BROWSER_SSO, auth.JWT] and not params.get('use_pure', False):
            #           params['auth_plugin'] = 'mysql_clear_password'
            params['ssl_verify_identity'] = True
            params['ssl_verify_cert'] = True

        return params

    def is_connected(self, conn: Any, reconnect: bool = False) -> bool:
        try:
            conn.ping(reconnect=reconnect)
            return True
        except conn.InterfaceError:
            return False
