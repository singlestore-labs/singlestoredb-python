#!/usr/bin/env python
"""SingleStoreDB Base Manager."""
import os
import re
import sys
import time
from copy import deepcopy
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union
from urllib.parse import urljoin

import jwt
import requests

from .. import config
from ..exceptions import ManagementError
from .utils import get_token


def set_organization(kwargs: Dict[str, Any]) -> None:
    """Set the organization ID in the dictionary."""
    if kwargs.get('params', {}).get('organizationID', None):
        return

    org = os.environ.get('SINGLESTOREDB_ORGANIZATION')
    if org:
        if 'params' not in kwargs:
            kwargs['params'] = {}
        kwargs['params']['organizationID'] = org


def is_jwt(token: str) -> bool:
    """Is the given token a JWT?"""
    try:
        jwt.decode(token, options={'verify_signature': False})
        return True
    except jwt.DecodeError:
        return False


class Manager(object):
    """SingleStoreDB manager base class."""

    #: Management API version if none is specified.
    default_version = config.get_option('management.version') or 'v1'

    #: Base URL if none is specified.
    default_base_url = config.get_option('management.base_url') \
        or 'https://api.singlestore.com'

    #: Object type
    obj_type = ''

    def __init__(
        self, access_token: Optional[str] = None, version: Optional[str] = None,
        base_url: Optional[str] = None, *, organization_id: Optional[str] = None,
    ):
        from .. import __version__ as client_version
        new_access_token = (
            access_token or get_token()
        )
        if not new_access_token:
            raise ManagementError(msg='No management token was configured.')

        self.version = version or self.default_version

        self._is_jwt = not access_token and new_access_token and is_jwt(new_access_token)
        self._sess = requests.Session()
        self._sess.headers.update({
            'Authorization': f'Bearer {new_access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': f'SingleStoreDB-Python/{client_version}',
        })

        self._base_url = (
            base_url
            or config.get_option('management.base_url')
            or type(self).default_base_url
        ) + '/'
        self._access_token = new_access_token
        self._params: Dict[str, str] = {}
        if organization_id:
            self._params['organizationID'] = organization_id

    def copy(self) -> 'Manager':
        """Create a new instance with the same settings."""
        new_manager = type(self).__new__(type(self))
        new_manager._is_jwt = self._is_jwt
        new_manager._sess = deepcopy(self._sess)
        new_manager._base_url = self._base_url
        new_manager.version = self.version
        new_manager._access_token = self._access_token
        new_manager._params = deepcopy(self._params)
        return new_manager

    def __getattr__(self, name: str) -> Any:
        """Handle dynamic version attributes (v2, v3, etc.)."""
        if re.match(r'^v\d+[0-9a-z]*$', name):
            new_mgr = self.copy()
            new_mgr.version = name
            return new_mgr
        return super().__getattribute__(name)

    def _check(
        self, res: requests.Response, url: str, params: Dict[str, Any],
    ) -> requests.Response:
        """
        Check the HTTP response status code and raise an exception as needed.

        Parameters
        ----------
        res : requests.Response
            HTTP response to check

        Returns
        -------
        requests.Response

        """
        if config.get_option('debug.queries'):
            print(os.path.join(self._base_url, url), params, file=sys.stderr)
        if res.status_code >= 400:
            txt = res.text.strip()
            msg = f'{txt}: /{url}'
            if params:
                new_params = params.copy()
                if 'json' in new_params:
                    for k, v in new_params['json'].items():
                        if 'password' in k.lower() and v:
                            new_params['json'][k] = '*' * len(v)
                msg += ': {}'.format(str(new_params))
            raise ManagementError(errno=res.status_code, msg=msg, response=txt)
        return res

    def _doit(
        self,
        method: str,
        path: str,
        *args: Any,
        **kwargs: Any,
    ) -> requests.Response:
        """Perform HTTP request."""
        # Refresh the JWT as needed
        if self._is_jwt:
            self._sess.headers.update({'Authorization': f'Bearer {get_token()}'})

        # Combine version and path
        versioned_path = f'{self._version}/{path}'

        return getattr(self._sess, method.lower())(
            urljoin(self._base_url, versioned_path), *args, **kwargs,
        )

    def _get(self, path: str, *args: Any, **kwargs: Any) -> requests.Response:
        """
        Invoke a GET request.

        Parameters
        ----------
        path : str
            Path of the resource
        *args : positional arguments, optional
            Arguments to add to the GET request
        **kwargs : keyword arguments, optional
            Keyword arguments to add to the GET request

        Returns
        -------
        requests.Response

        """
        if self._params:
            kwargs['params'] = self._params
        set_organization(kwargs)
        return self._check(self._doit('get', path, *args, **kwargs), path, kwargs)

    def _post(self, path: str, *args: Any, **kwargs: Any) -> requests.Response:
        """
        Invoke a POST request.

        Parameters
        ----------
        path : str
            Path of the resource
        *args : positional arguments, optional
            Arguments to add to the POST request
        **kwargs : keyword arguments, optional
            Keyword arguments to add to the POST request

        Returns
        -------
        requests.Response

        """
        if self._params:
            kwargs['params'] = self._params
        set_organization(kwargs)
        return self._check(self._doit('post', path, *args, **kwargs), path, kwargs)

    def _put(self, path: str, *args: Any, **kwargs: Any) -> requests.Response:
        """
        Invoke a PUT request.

        Parameters
        ----------
        path : str
            Path of the resource
        *args : positional arguments, optional
            Arguments to add to the POST request
        **kwargs : keyword arguments, optional
            Keyword arguments to add to the POST request

        Returns
        -------
        requests.Response

        """
        if self._params:
            kwargs['params'] = self._params
        set_organization(kwargs)
        return self._check(self._doit('put', path, *args, **kwargs), path, kwargs)

    def _delete(self, path: str, *args: Any, **kwargs: Any) -> requests.Response:
        """
        Invoke a DELETE request.

        Parameters
        ----------
        path : str
            Path of the resource
        *args : positional arguments, optional
            Arguments to add to the DELETE request
        **kwargs : keyword arguments, optional
            Keyword arguments to add to the DELETE request

        Returns
        -------
        requests.Response

        """
        if self._params:
            kwargs['params'] = self._params
        set_organization(kwargs)
        return self._check(self._doit('delete', path, *args, **kwargs), path, kwargs)

    def _patch(self, path: str, *args: Any, **kwargs: Any) -> requests.Response:
        """
        Invoke a PATCH request.

        Parameters
        ----------
        path : str
            Path of the resource
        *args : positional arguments, optional
            Arguments to add to the PATCH request
        **kwargs : keyword arguments, optional
            Keyword arguments to add to the PATCH request

        Returns
        -------
        requests.Response

        """
        if self._params:
            kwargs['params'] = self._params
        set_organization(kwargs)
        return self._check(self._doit('patch', path, *args, **kwargs), path, kwargs)

    def _wait_on_state(
        self,
        out: Any,
        state: Union[str, List[str]],
        interval: int = 20,
        timeout: int = 600,
    ) -> Any:
        """
        Wait on server state before continuing.

        Parameters
        ----------
        out : Any
            Current object
        state : str or List[str]
            State(s) to wait for
        interval : int, optional
            Interval between each server poll
        timeout : int, optional
            Maximum time to wait before raising an exception

        Raises
        ------
        ManagementError
            If timeout is reached

        Returns
        -------
        Same object type as `out`

        """
        states = [
            x.lower().strip()
            for x in (isinstance(state, str) and [state] or state)
        ]

        if getattr(out, 'state', None) is None:
            raise ManagementError(
                msg='{} object does not have a `state` attribute'.format(
                    type(out).__name__,
                ),
            )

        while True:
            if getattr(out, 'state').lower() in states:
                break
            if timeout <= 0:
                raise ManagementError(
                    msg=f'Exceeded waiting time for {self.obj_type} to become '
                        '{}.'.format(', '.join(states)),
                )
            time.sleep(interval)
            timeout -= interval
            out = getattr(self, f'get_{self.obj_type}')(out.id)

        return out
