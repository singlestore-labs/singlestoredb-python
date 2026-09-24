#!/usr/bin/env python
"""SingleStoreDB Base Manager."""
import functools
import logging
import os
import random
import re
import sys
import time
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from . import timing
from .. import config
from ..exceptions import ManagementError
from ..exceptions import OperationalError
from ._version_import import DEFAULT_VERSION
from .utils import get_token


logger = logging.getLogger(__name__)


def set_organization(kwargs: Dict[str, Any]) -> None:
    """Set the organization ID in the dictionary."""
    if kwargs.get('params', {}).get('organizationID', None):
        return

    org = os.environ.get('SINGLESTOREDB_ORGANIZATION')
    if org:
        if 'params' not in kwargs:
            kwargs['params'] = {}
        kwargs['params']['organizationID'] = org


#: Methods that may be replayed after a transport-level failure. POST is
#: absent on purpose: a dropped connection does not say whether the server
#: acted on the request, and replaying ``POST /clusters`` would deploy twice.
#: Everything the long ``wait_on_*`` loops issue is a GET, so the retries
#: cover the failure mode that actually shows up -- a keep-alive connection
#: the far end closed while the client was sleeping between polls, which
#: surfaces as ``RemoteDisconnected`` on the next request.
#:
#: The one exception is a creation the organization lock blocked, which
#: :func:`retry_on_lock` replays: it is identified by the error message, which
#: this policy never sees.
RETRY_METHODS = frozenset(['GET', 'HEAD', 'OPTIONS', 'PUT', 'DELETE'])

#: Status codes worth retrying. These are the transient ones; a 4xx other
#: than 429 is a client error that will fail again identically.
RETRY_STATUSES = frozenset([429, 500, 502, 503, 504])


def build_retry(
    total: Optional[int] = None,
    backoff_factor: Optional[float] = None,
) -> Retry:
    """Build the retry policy used by every manager session."""
    if total is None:
        total = int(os.environ.get('SINGLESTOREDB_MANAGEMENT_RETRIES', '4'))
    if backoff_factor is None:
        backoff_factor = float(
            os.environ.get('SINGLESTOREDB_MANAGEMENT_RETRY_BACKOFF', '0.5'),
        )
    return Retry(
        total=total,
        connect=total,
        read=total,
        status=total,
        allowed_methods=RETRY_METHODS,
        status_forcelist=RETRY_STATUSES,
        backoff_factor=backoff_factor,
        # Let ``Manager._check`` raise the error with the response body in it
        # rather than urllib3 raising a bare MaxRetryError.
        raise_on_status=False,
        respect_retry_after_header=True,
    )


#: "could not acquire lock within duration", the API's refusal to start a
#: creation while another one in the organization holds the lock. Seen from
#: ``POST /workspaceGroups`` and ``POST /clusters``, whose message says "error
#: creating workspace" either way, so the match cannot key on the noun.
#:
#: Matched on the message, not the status: the conflict arrives as a 500, and so
#: does a name collision. The wording is also the only part that says nothing
#: was created, which is what makes replaying the POST safe.
LOCK_ERROR_RE = re.compile(r'acquire[^.]{0,40}lock', re.I)

#: Ceiling on the wait between lock retries, and the random extra added to each
#: one. Capped because what is being waited out is another creation's POST
#: returning, not a deployment coming up. Jittered because two clients that
#: collide back off by the same amounts from the same moment -- two xdist
#: workers, say -- and would otherwise retry in step indefinitely.
LOCK_RETRY_MAX_INTERVAL = 60.0
LOCK_RETRY_JITTER = 5.0


def lock_retry_policy() -> Tuple[int, float]:
    """
    Return the (retries, interval) applied to an organization lock conflict.

    ``retries`` counts attempts *after* the first, so the defaults wait 20, 40,
    60, 60 and 60 seconds -- four minutes at worst, small enough that a stuck
    organization fails rather than idling out a CI job's timeout.

    Set ``SINGLESTOREDB_MANAGEMENT_LOCK_RETRIES=0`` to raise the conflict at
    once instead.
    """
    return (
        int(os.environ.get('SINGLESTOREDB_MANAGEMENT_LOCK_RETRIES', '5')),
        float(
            os.environ.get('SINGLESTOREDB_MANAGEMENT_LOCK_RETRY_INTERVAL', '20'),
        ),
    )


def is_lock_error(exc: BaseException) -> bool:
    """Is this error the organization refusing to take the lock?"""
    return bool(LOCK_ERROR_RE.search(str(exc)))


def lock_retry_wait(attempt: int, interval: float) -> float:
    """Seconds to wait before replaying a creation that lost the lock."""
    return min(interval * attempt, LOCK_RETRY_MAX_INTERVAL) + \
        random.uniform(0, LOCK_RETRY_JITTER)


def retry_on_lock(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Wait out an organization lock conflict on a deployment creation.

    Replaying a POST is safe here where widening :data:`RETRY_METHODS` would not
    be: the transport sees only a 500 and cannot know whether the server acted,
    whereas the lock message says the creation never started. A creation that
    made something and *then* failed does not come back with this message, and
    would surface on the replay as a name conflict rather than being swallowed.

    Worn by ``WorkspaceManager.create_workspace_group`` and
    ``ClusterManager.create_cluster`` only -- the two calls that contend for the
    lock. Fusion SQL's ``CREATE WORKSPACE GROUP`` and ``CREATE CLUSTER`` go
    through them, so they are covered too.

    Any other ``ManagementError`` is raised at once, as is the conflict itself
    once :func:`lock_retry_policy`'s budget runs out.
    """
    @functools.wraps(func)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        retries, interval = lock_retry_policy()
        attempt = 0
        while True:
            try:
                return func(self, *args, **kwargs)
            except ManagementError as exc:
                attempt += 1
                if attempt > retries or not is_lock_error(exc):
                    raise
                wait = lock_retry_wait(attempt, interval)
                logger.info(
                    f'{func.__name__} could not take the organization lock '
                    f'({exc}); attempt {attempt} of {retries + 1}, retrying '
                    f'in {wait:.1f}s',
                )
                timing.sleep(wait, f'{func.__name__} organization lock')

    # Says which methods wear this, for a test to assert against. On the
    # wrapper's ``__dict__``, so ``functools.wraps`` carries it outward through
    # any later decorator -- the test suite wraps these methods again to track
    # what a run has deployed.
    wrapper.__retry_on_lock__ = True  # type: ignore[attr-defined]

    return wrapper


def default_timeout() -> Tuple[float, float]:
    """
    Return the (connect, read) timeout applied when a caller gives none.

    Without this a stalled connection hangs the client forever instead of
    failing and being retried.
    """
    return (
        float(os.environ.get('SINGLESTOREDB_MANAGEMENT_CONNECT_TIMEOUT', '10')),
        float(os.environ.get('SINGLESTOREDB_MANAGEMENT_READ_TIMEOUT', '180')),
    )


def is_jwt(token: str) -> bool:
    """Is the given token a JWT?"""
    import jwt
    try:
        jwt.decode(token, options={'verify_signature': False})
        return True
    except jwt.DecodeError:
        return False


class Manager:
    """SingleStoreDB manager base class."""

    #: Management API version if none is specified. The shared
    #: :data:`~singlestoredb.management._version_import.DEFAULT_VERSION`, which
    #: also supplies the ``management.version`` option default, so the two
    #: cannot drift. Deliberately not a reading of that option: it is read by
    #: the ``manage_*`` factories at call time, and reading it here would let a
    #: version-specific class declare itself to be whatever the option happened
    #: to say. A class that implements one specific version pins that version
    #: as a literal instead of inheriting this.
    default_version = DEFAULT_VERSION

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

        base_url_root = (
            base_url
            or config.get_option('management.base_url')
            or type(self).default_base_url
        )

        self._is_jwt = not access_token and new_access_token and is_jwt(new_access_token)
        self._sess = requests.Session()
        adapter = HTTPAdapter(max_retries=build_retry())
        self._sess.mount('http://', adapter)
        self._sess.mount('https://', adapter)
        self._sess.headers.update({
            'Authorization': f'Bearer {new_access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': f'SingleStoreDB-Python/{client_version}',
        })

        self._base_url = urljoin(
            base_url_root,
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
        # Refresh the JWT as needed
        if self._is_jwt:
            self._sess.headers.update({'Authorization': f'Bearer {get_token()}'})
        kwargs.setdefault('timeout', default_timeout())
        url = urljoin(self._base_url, path)
        # Every management HTTP call comes through here, so this is the one
        # place request time has to be recorded. See management.timing.
        started_at = time.monotonic()
        try:
            res = getattr(self._sess, method.lower())(url, *args, **kwargs)
        except requests.exceptions.RequestException as exc:
            timing.record_request(
                method, path, time.monotonic() - started_at, started_at,
                error=exc,
            )
            # A transport failure otherwise escapes as a bare
            # requests.ConnectionError / ReadTimeout naming neither the route
            # nor the method, which makes it indistinguishable from a bug in
            # the caller. Retries for the replayable methods are already
            # exhausted by the time this is reached.
            raise ManagementError(
                msg=f'{type(exc).__name__} on {method.upper()} {url}: {exc}',
            ) from exc
        timing.record_request(
            method, path, time.monotonic() - started_at, started_at,
            response=res,
        )
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
        if self._params:
            params = dict(self._params)
            params.update(kwargs.get('params', {}))
            kwargs['params'] = params
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
            params = dict(self._params)
            params.update(kwargs.get('params', {}))
            kwargs['params'] = params
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
            params = dict(self._params)
            params.update(kwargs.get('params', {}))
            kwargs['params'] = params
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
            params = dict(self._params)
            params.update(kwargs.get('params', {}))
            kwargs['params'] = params
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
            params = dict(self._params)
            params.update(kwargs.get('params', {}))
            kwargs['params'] = params
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

        remaining = float(timeout)
        while True:
            if getattr(out, 'state').lower() in states:
                break
            if remaining <= 0:
                raise ManagementError(
                    msg=f'Exceeded waiting time for {self.obj_type} to become '
                        '{}.'.format(', '.join(states)),
                )
            started_at = timing.now()
            timing.sleep(
                interval,
                '{} state -> {}'.format(self.obj_type, ', '.join(states)),
            )
            out = getattr(self, f'get_{self.obj_type}')(out.id)
            # Charged after the refetch, and by measured time: the refetch is
            # part of what the iteration cost. See timing.poll_cost.
            remaining -= timing.poll_cost(started_at, interval)

        return out

    def _wait_on_endpoint(
        self,
        out: Any,
        interval: int = 10,
        timeout: int = 300,
    ) -> Any:
        """
        Wait for the endpoint to be ready by attempting to connect.

        Parameters
        ----------
        out : Any
            Deployment object with a connect method -- a ``Cluster`` or
            ``StarterCluster`` at v2, a ``Workspace`` at v1
        interval : int, optional
            Interval between each connection attempt (default: 10 seconds)
        timeout : int, optional
            Maximum time to wait before raising an exception (default: 300 seconds)

        Raises
        ------
        ManagementError
            If timeout is reached or endpoint is not available

        Returns
        -------
        Same object type as `out`

        """
        # Only wait if workload type is set which means we are in the
        # notebook environment. Outside of the environment, the endpoint
        # may not be reachable directly.
        if not os.environ.get('SINGLESTOREDB_WORKLOAD_TYPE', ''):
            return out

        if not hasattr(out, 'connect') or not out.connect:
            raise ManagementError(
                msg=f'{type(out).__name__} object does not have a valid endpoint',
            )

        remaining = float(timeout)
        while True:
            started_at = timing.now()
            try:
                # Try to establish a connection to the endpoint using context manager
                with timing.timed(f'{self.obj_type} endpoint connect'):
                    with out.connect(connect_timeout=5):
                        pass
                # Connected, so the endpoint is ready. Without this the loop
                # reconnects forever on success and only ever leaves through
                # the 1045 branch or the timeout.
                break
            except Exception as exc:
                # If we get an 'access denied' error, that means that the server is
                # up and we just aren't authenticating.
                if isinstance(exc, OperationalError) and exc.errno == 1045:
                    break
                # If connection fails, check timeout and retry
                if remaining <= 0:
                    raise ManagementError(
                        msg=f'Exceeded waiting time for {self.obj_type} endpoint '
                            'to become ready',
                    )
                timing.sleep(interval, f'{self.obj_type} endpoint')
                # The failed connect attempt is part of what the iteration
                # cost: connect_timeout is 5 seconds on top of the sleep.
                remaining -= timing.poll_cost(started_at, interval)

        return out
