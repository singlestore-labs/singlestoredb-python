#!/usr/bin/env python
"""SingleStoreDB Base Manager."""
import time
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union
from urllib.parse import urljoin

import requests

from .. import config
from ..exceptions import ManagementError


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
        base_url: Optional[str] = None,
    ):
        access_token = (
            access_token or
            config.get_option('management.token')
        )
        if not access_token:
            raise ManagementError(msg='No management token was configured.')
        self._sess = requests.Session()
        self._sess.headers.update({
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        })
        self._base_url = urljoin(
            base_url or type(self).default_base_url,
            version or type(self).default_version,
        ) + '/'

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
        if res.status_code >= 400:
            txt = res.text.strip()
            msg = f'{txt}: /{url}'
            if params:
                new_params = params.copy()
                if 'json' in new_params:
                    for k, v in new_params['json'].items():
                        if 'password' in k.lower():
                            new_params['json'][k] = '*' * len(v)
                msg += ': {}'.format(str(new_params))
            raise ManagementError(errno=res.status_code, msg=msg)
        return res

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
        return self._check(
            self._sess.get(
                urljoin(self._base_url, path),
                *args, **kwargs,
            ),
            path, kwargs,
        )

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
        return self._check(
            self._sess.post(
                urljoin(self._base_url, path),
                *args, **kwargs,
            ),
            path, kwargs,
        )

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
        return self._check(
            self._sess.delete(
                urljoin(self._base_url, path),
                *args, **kwargs,
            ),
            path, kwargs,
        )

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
        return self._check(
            self._sess.patch(
                urljoin(self._base_url, path),
                *args, **kwargs,
            ),
            path, kwargs,
        )

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
