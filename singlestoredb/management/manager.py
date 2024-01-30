#!/usr/bin/env python
"""SingleStoreDB Base Manager."""
import os
import sys
import time
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
from .utils import get_organization
from .utils import get_token


def set_organization(kwargs: Dict[str, Any]) -> None:
    """Set the organization ID in the dictionary."""
    if kwargs.get('params', {}).get('organizationID', None):
        return

    org = get_organization()
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
    default_version = 'v1'

    #: Base URL if none is specified.
    default_base_url = 'https://api.singlestore.com'

    #: Object type
    obj_type = ''

    def __init__(
        self, access_token: Optional[str] = None, version: Optional[str] = None,
        base_url: Optional[str] = None, *, organization_id: Optional[str] = None,
    ):
        from .. import __version__ as client_version
        access_token = (
            access_token or get_token()
        )
        if not access_token:
            raise ManagementError(msg='No management token was configured.')
        self._sess = requests.Session()
        self._sess.headers.update({
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': f'SingleStoreDB-Python/{client_version}',
        })
        self._base_url = urljoin(
            base_url or type(self).default_base_url,
            version or type(self).default_version,
        ) + '/'
        self._params: Dict[str, str] = {}
        if organization_id:
            self._params['organizationID'] = organization_id

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
        return getattr(self._sess, method.lower())(
            urljoin(self._base_url, path), *args, **kwargs,
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
