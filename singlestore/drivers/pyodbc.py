from __future__ import annotations

from typing import Any
from typing import Dict

from .base import Driver


class PyODBCDriver(Driver):

    name = 'pyodbc'

    pkg_name = 'pyodbc'
    pypi = 'pyodbc'
    anaconda = 'pyodbc'

    odbc_driver = 'SingleStore ODBC 1.0 Unicode Driver'
    odbc_driver = 'SingleStore ODBC Driver'

    def remap_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return dict(
            server=params['host'],
            port=params['port'] or 3306,
            database=params['database'],
            uid=params['user'],
            pwd=params['password'],
            #           charset=params['charset'],
            #           driver=params['driver'] or type(self).odbc_driver,
            driver=type(self).odbc_driver,
        )
