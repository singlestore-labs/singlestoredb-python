from __future__ import annotations

import warnings
from typing import Any
from typing import Dict

from .base import Driver


class HTTPDriver(Driver):

    name = 'http'

    pkg_name = 'singlestoredb.http'
    pypi = 'singlestoredb'
    anaconda = 'singlestore::singlestoredb'

    def remap_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        params.pop('pure_python', False)
        params.pop('charset', None)
        params.pop('odbc_driver', None)
        params.pop('credential_type', None)

        params['protocol'] = params.pop('driver', '').replace(
            'singlestoredb+', '',
        ) or None

        if params.pop('local_infile', False):
            warnings.warn('The HTTP driver does not support file uploads.')

        if params['port'] is None:
            if type(self).name == 'https':
                params['port'] = 443
            else:
                params['port'] = 80

        self.converters = params.pop('converters', {})

        return params

    def is_connected(self, conn: Any, reconnect: bool = False) -> bool:
        return conn.is_connected()


class HTTPSDriver(HTTPDriver):

    name = 'https'
