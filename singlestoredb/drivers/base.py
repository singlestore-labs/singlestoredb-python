from __future__ import annotations

import sys
from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional

from .. import exceptions


def wrap_converter(
    outer: Optional[Callable[[Any], Any]],
    conv: Optional[Callable[[Any], Any]],
) -> Optional[Callable[[Any], Any]]:
    """Create a pipeline from two functions."""
    if outer is None:
        return conv

    if conv is None:
        return outer

    def converter(value: Any) -> Any:
        return outer(conv(value))  # type: ignore

    return converter


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

    # Does the driver return bytes for all data values?
    # If true and flags are available in the description to deterimine
    # if a character field is text or binary, the text will be automatically
    # decoded before conversion functions are run.
    returns_bytes: bool = False

    def __init__(self, **kwargs: Any):
        self._params = kwargs

        # These converters get applied after the driver does its own
        # conversions. This allows each driver to decide which conversions
        # get handled internally and which get applied by this framework.
        self.converters: Dict[int, Callable[[Any], Any]] = {}

    def connect(self) -> Any:
        """Create a new connection."""
        params = {
            k: v for k, v in self.remap_params(self._params).items() if v is not None
        }
        conn = self.dbapi.connect(**params)
        self.after_connect(conn, self._params)
        return conn

    def merge_converters(
        self,
        user_converters: Dict[int, Callable[[Any], Any]],
        driver_converters: Dict[int, Callable[[Any], Any]],
    ) -> Dict[int, Callable[[Any], Any]]:
        """Merge two sets of converters into pipelines as needed."""
        out = dict(driver_converters)
        for key, value in user_converters.items():
            func = wrap_converter(value, out.get(key))
            if func is not None:
                out[key] = func
        return out

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

    def convert_exception(self, exc: Exception) -> Exception:
        """Convert driver-specific exception to SingleStoreDB exception."""
        dbapi = self.dbapi
        if not isinstance(exc, (dbapi.Error, dbapi.Warning)):
            return exc
        new_exc: Optional[type] = None
        if isinstance(exc, dbapi.NotSupportedError):
            new_exc = exceptions.NotSupportedError
        elif isinstance(exc, dbapi.ProgrammingError):
            new_exc = exceptions.ProgrammingError
        elif isinstance(exc, dbapi.InternalError):
            new_exc = exceptions.InternalError
        elif isinstance(exc, dbapi.IntegrityError):
            new_exc = exceptions.IntegrityError
        elif isinstance(exc, dbapi.OperationalError):
            new_exc = exceptions.OperationalError
        elif isinstance(exc, dbapi.DataError):
            new_exc = exceptions.DataError
        elif isinstance(exc, dbapi.DatabaseError):
            new_exc = exceptions.DatabaseError
        elif isinstance(exc, dbapi.InterfaceError):
            new_exc = exceptions.InterfaceError
        elif isinstance(exc, dbapi.Error):
            new_exc = exceptions.Error
        elif isinstance(exc, dbapi.Warning):
            new_exc = exceptions.Warning
        if new_exc is None:
            return exc

        # Check for exceptions with errno / msg first
        errno = getattr(exc, 'errno', None)
        msg = getattr(exc, 'msg', None)
        if msg:
            return new_exc(
                errno=errno, msg=msg,
                sqlstate=getattr(exc, 'sqlstate', None),
            )

        # Check for exceptions with just args
        args = getattr(exc, 'args', [])
        if len(args) > 1:
            return new_exc(args[0], args[1])
        if len(args):
            return new_exc(args[0])

        # Don't know what type it is
        raise ValueError(f'Unrecognized exception format: {exc}')

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
