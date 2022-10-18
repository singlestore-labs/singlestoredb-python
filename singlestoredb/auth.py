#!/usr/bin/env python
import datetime
from typing import Any
from typing import List
from typing import Optional
from typing import Union

import jwt


# Credential types
PASSWORD = 'password'
JWT = 'jwt'
BROWSER_SSO = 'browser_sso'

# Single Sign-On URL
SSO_URL = 'https://portal.singlestore.com/engine-sso'


class JSONWebToken(object):
    """Container for JWT information."""

    def __init__(
        self, token: str, expires: datetime.datetime,
        email: str, username: str, url: str = SSO_URL,
        clusters: Optional[Union[str, List[str]]] = None,
        databases: Optional[Union[str, List[str]]] = None,
        timeout: int = 60,
    ):
        self.token = token
        self.expires = expires
        self.email = email
        self.username = username
        self.model_version_number = 1

        # Attributes needed for refreshing tokens
        self.url = url
        self.clusters = clusters
        self.databases = databases
        self.timeout = timeout

    @classmethod
    def from_token(cls, token: bytes, verify_signature: bool = False) -> 'JSONWebToken':
        """Validate the contents of the JWT."""
        info = jwt.decode(token, options={'verify_signature': verify_signature})

        if not info.get('sub', None) and not info.get('username', None):
            raise ValueError("Missing 'sub' and 'username' in claims")
        if not info.get('email', None):
            raise ValueError("Missing 'email' in claims")
        if not info.get('exp', None):
            raise ValueError("Missing 'exp' in claims")
        try:
            expires = datetime.datetime.fromtimestamp(info['exp'], datetime.timezone.utc)
        except Exception as exc:
            raise ValueError("Invalid 'exp' in claims: {}".format(str(exc)))

        username = info.get('username', info.get('sub', None))
        email = info['email']

        return cls(token.decode('utf-8'), expires=expires, email=email, username=username)

    def __str__(self) -> str:
        return self.token

    def __repr__(self) -> str:
        return repr(self.token)

    @property
    def is_expired(self) -> bool:
        """Determine if the token has expired."""
        return self.expires >= datetime.datetime.now()

    def refresh(self, force: bool = False) -> bool:
        """
        Refresh the token as needed.

        Parameters
        ----------
        force : bool, optional
           Should a new token be generated even if the existing
           one has not expired yet?

        Returns
        -------
        bool : Indicating whether the token was refreshed or not

        """
        if force or self.is_expired:
            out = get_jwt(
                self.email, url=self.url, clusters=self.clusters,
                databases=self.databases, timeout=self.timeout,
            )
            self.token = out.token
            self.expires = out.expires
            return True
        return False


def _listify(s: Optional[Union[str, List[str]]]) -> Optional[str]:
    """Return a list of strings in a comma-separated string."""
    if s is None:
        return None
    if not isinstance(s, str):
        return ','.join(s)
    return s


def get_jwt(
    email: str, url: str = SSO_URL,
    clusters: Optional[Union[str, List[str]]] = None,
    databases: Optional[Union[str, List[str]]] = None,
    timeout: int = 60, browser: Optional[Union[str, List[str]]] = None,
) -> JSONWebToken:
    """
    Retrieve a JWT token from the SingleStoreDB single-sign-on URL.

    Parameters
    ----------
    email : str
        EMail of the database user
    url : str, optional
        The URL of the single-sign-on token generator
    clusters : str or list[str], optional
        The name of the cluster being connected to
    databases : str or list[str], optional
        The name of the database being connected to
    timeout : int, optional
        Number of seconds to wait before timing out the authentication request
    browser : str or list[str], optional
        Browser to use instead of the default. This value can be any of the
        names specified in Python's `webbrowser` module. This includes
        'google-chrome', 'chrome', 'chromium', 'chromium-browser', 'firefox',
        etc. Note that at the time of this writing, Safari was not
        compatible. If a list of names is specified, each one tried until
        a working browser is located.

    Returns
    -------
    JSONWebToken

    """
    import platform
    import webbrowser
    import time
    import threading
    import urllib
    from http.server import BaseHTTPRequestHandler, HTTPServer

    from .config import get_option

    token = []
    error = []

    class AuthServer(BaseHTTPRequestHandler):

        def log_message(self, format: str, *args: Any) -> None:
            return

        def do_POST(self) -> None:
            content_len = int(self.headers.get('Content-Length', 0))
            post_body = self.rfile.read(content_len)

            try:
                out = JSONWebToken.from_token(post_body)
            except Exception as exc:
                self.send_response(400, exc.args[0])
                self.send_header('Content-Type', 'text/plain')
                self.end_headers()
                error.append(exc)
                return

            token.append(out)

            self.send_response(204)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()

    server = None

    try:
        server = HTTPServer(('127.0.0.1', 0), AuthServer)
        threading.Thread(target=server.serve_forever).start()

        query = urllib.parse.urlencode({
            k: v for k, v in dict(
                email=email,
                returnTo=f'http://{server.server_address[0]}:{server.server_address[1]}',
                db=_listify(databases),
                cluster=_listify(clusters),
            ).items() if v is not None
        })

        if browser is None:
            browser = get_option('sso_browser')

        # On Mac, always specify a list of browsers to check because Safari
        # is not compatible.
        if browser is None and platform.platform().lower().startswith('mac'):
            browser = [
                'chrome', 'google-chrome', 'chromium',
                'chromium-browser', 'firefox',
            ]

        if browser and isinstance(browser, str):
            browser = [browser]

        if browser:
            exc: Optional[Exception] = None
            for item in browser:
                try:
                    webbrowser.get(item).open(f'{url}?{query}')
                    break
                except webbrowser.Error as wexc:
                    exc = wexc
                    pass
            if exc is not None:
                raise RuntimeError(
                    'Could not find compatible web browser for accessing JWT',
                )
        else:
            webbrowser.open(f'{url}?{query}')

        for i in range(timeout * 2):
            if error:
                raise error[0]
            if token:
                out = token[0]
                out.url = url
                out.clusters = clusters
                out.databases = databases
                out.timeout = timeout
                return out
            time.sleep(0.5)

    finally:
        if server is not None:
            server.shutdown()

    raise RuntimeError('Timeout waiting for token')
