from __future__ import annotations

import sys
from typing import Any
from typing import Dict


class Driver(object):
    """Base driver class."""

    # Name of driver used in connections
    name: str = ''

    # Name of package to import
    pkg_name: str = ''

    # Name of the package on PyPI.org
    pypi: str = ''

    # Name of the package on Anaconda.org
    anaconda: str = ''

    def __init__(self, **kwargs: Any):
        self._params = kwargs

    def connect(self) -> Any:
        """Create a new connection."""
        params = {
            k: v for k, v in self.remap_params(self._params).items() if v is not None
        }
        conn = self.dbapi.connect(**params)
        self.after_connect(conn, self._params)
        return conn

    def after_connect(self, conn: Any, params: Dict[str, Any]) -> None:
        """
        Callback for immediately after making a connection.

        Parameters
        ----------
        conn : Connection
            Connection object
        params : dict
            Original connection parameters

        """
        return

    def is_connected(self, conn: Any, reconnect: bool = False) -> bool:
        """
        Determine if the server is still connected.

        Parameters
        ----------
        conn : Connection
            Connection object to test
        reconnect : bool, optional
            If the server is not connected, should a reconnection be attempted?

        Returns
        -------
        bool

        """
        raise NotImplementedError

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
                if '::' in type(self).anaconda:
                    msg.append(
                        " or 'conda install -c {} {}' (for Anaconda)"
                        .format(*type(self).anaconda.split('::', 1)),
                    )
                else:
                    msg.append(
                        " or 'conda install {}' (for Anaconda)"
                        .format(type(self).anaconda),
                    )
            msg.append(' to install it.')
            print(''.join(msg), file=sys.stderr)
            raise
