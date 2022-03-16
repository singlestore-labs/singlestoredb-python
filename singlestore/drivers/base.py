from __future__ import annotations

import sys
from typing import Any
from typing import Dict
from typing import Optional


class Driver(object):
    """
    Base driver class.

    Parameters
    ----------
    host : str, optional
        Hostname or IP address of the server
    user : str, optional
        Database user name
    password : str, optional
        Database user password
    port : int, optional
        Database port
    database : str, optional
        Database name
    local_infile : bool, optional
        Should local files be allowed to be uploaded?
    pure_python : bool, optional
        Use the connector in pure Python mode?
    driver : str, optional
        ODBC driver name (if using an ODBC interface)

    """

    # Name of driver used in connections
    name: str = ''

    # Name of package to import
    pkg_name: str = ''

    # Name of the package on PyPI.org
    pypi: str = ''

    # Name of the package on Anaconda.org
    anaconda: str = ''

    def __init__(
        self, host: Optional[str] = None,
        user: Optional[str] = None, password: Optional[str] = None,
        port: Optional[int] = None, database: Optional[str] = None,
        local_infile: bool = False, pure_python: bool = False,
        driver: Optional[str] = None,
    ):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.database = database
        self.local_infile = local_infile
        self.pure_python = pure_python
        self.driver = driver

    def connect(self) -> Any:
        """Create a new connection."""
        params = {
            k: v for k, v in self.remap_params(
                dict(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    local_infile=self.local_infile,
                    pure_python=self.pure_python,
                    driver=self.driver,
                ),
            ).items() if v is not None
        }
        return self.dbapi.connect(**params)

    def remap_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map generalized parameters to package-specific parameters.

        Parameters
        ----------
        params : dict
            Dictionary of connection parameters.

        Returns
        -------
        dict

        """
        return params

    @property
    def dbapi(self) -> Any:
        """Return imported DB-API-compatible module."""
        if not type(self).pkg_name:
            raise ValueError('No package name defined in driver.')
        try:
            return __import__(type(self).pkg_name, {}, {}, ['connect'])
        except ModuleNotFoundError:
            msg = []
            msg.append('{} is not available.'.format(type(self).pkg_name))
            msg.append(" Use 'pip install {}'".format(type(self).pypi))
            if type(self).anaconda:
                msg.append(
                    " or 'conda install {}' (for Anaconda)"
                    .format(type(self).anaconda),
                )
            msg.append(' to install it.')
            print(''.join(msg), file=sys.stderr)
            raise
