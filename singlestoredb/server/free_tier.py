#!/usr/bin/env python
"""Utilities for running SingleStoreDB in the free tier."""
from __future__ import annotations

import atexit
import os
import platform
import signal
import subprocess
import urllib.parse
from types import TracebackType
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Type

import requests

from .. import connect
from ..connection import Connection

try:
    import pymongo
    has_pymongo = True
except ImportError:
    has_pymongo = False


class SingleStoreDB:
    """
    Manager for SingleStoreDB server running in Docker.

    Parameters
    -----------
    global_vars : dict, optional
        Global variables to set in the SingleStoreDB server.
    init_sql : str, optional
        Path to an SQL file to run on startup.

    """

    user: str
    password: str
    kai_enabled: bool
    server_port: int
    data_api_port: int
    kai_port: Optional[int]

    def __init__(self) -> None:
        r = requests.get('https://shell.singlestore.com/api/session')

        self._cookies = r.cookies.get_dict()

        if 'userSessionID' in self._cookies:
            self._session_id = self._cookies['userSessionID']
        else:
            self._session_id = ''

        d = r.json()

        self._connected = True
        self.kai_enabled = True
        self.kai_port = 27017
        self.server_port = 3333
        self.data_api_port = 443
        self.user = d['user']
        self.password = d['password']
        self._database = d['databaseName']
        self._endpoint = d['endpoint']
        self._workspace_id = d['workspaceID']

        self._saved_server_urls: Dict[str, Optional[str]] = {}

        # Make sure container gets cleaned up at exit
        atexit.register(self.stop)
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

        self._set_server_urls()

    def __str__(self) -> str:
        return f"SingleStoreDB('{self.connection_url}')"

    def __repr__(self) -> str:
        return str(self)

    def _set_server_urls(self) -> None:
        self._saved_server_urls['DATABASE_URL'] = os.environ.get('DATABASE_URL')
        os.environ['DATABASE_URL'] = self.connection_url
        self._saved_server_urls['SINGLESTOREDB_URL'] = os.environ.get('SINGLESTOREDB_URL')
        os.environ['SINGLESTOREDB_URL'] = self.connection_url

    def _restore_server_urls(self) -> None:
        try:
            for k, v in self._saved_server_urls.items():
                if v is None:
                    del os.environ[k]
                else:
                    os.environ[k] = v
        except KeyError:
            pass

    def logs(self) -> List[str]:
        return []

    @property
    def connection_url(self) -> str:
        """Connection URL for the SingleStoreDB server."""
        dbname = f'/{self._database}' if self._database else ''
        password = urllib.parse.quote_plus(self.password)
        return f'singlestoredb://{self.user}:{password}@' + \
               f'{self._endpoint}:{self.server_port}{dbname}'

    @property
    def http_connection_url(self) -> str:
        """HTTP Connection URL for the SingleStoreDB server."""
        dbname = f'/{self._database}' if self._database else ''
        password = urllib.parse.quote_plus(self.password)
        return f'singlestoredb+https://{self.user}:{password}@' + \
               f'{self._endpoint}:{self.data_api_port}{dbname}'

    def connect(
        self,
        use_data_api: bool = False,
        **kwargs: Any,
    ) -> Connection:
        """
        Connect to the SingleStoreDB server.

        Parameters
        -----------
        use_data_api : bool, optional
            Use the Data API for the connection.
        **kwargs : Any, optional
            Additional keyword arguments to pass to the connection.

        Returns
        --------
        Connection : Connection to the SingleStoreDB server.

        """
        if use_data_api:
            return connect(self.http_connection_url, **kwargs)
        return connect(self.connection_url, **kwargs)

    @property
    def kai_url(self) -> Optional[str]:
        """Connection URL for the Kai (MongoDB) server."""
        if not self.kai_enabled:
            return None
        password = urllib.parse.quote_plus(self.password)
        endpoint = self._endpoint.replace('shared-dml', 'shared-mongo')
        return f'mongodb://{self.user}^{self._database}:{password}@' + \
               f'{endpoint}:{self.kai_port}/' + \
               '?authMechanism=PLAIN&tls=true&loadBalanced=true' + \
               f'&dbName={self._database}'

    def connect_kai(self) -> 'pymongo.MongoClient':
        """Connect to the Kai (MongoDB) server."""
        if not self.kai_enabled:
            raise RuntimeError('kai is not enabled')
        if not has_pymongo:
            raise RuntimeError('pymongo is not installed')
        return pymongo.MongoClient(self.kai_url)

    def open_shell(self) -> None:
        """Open a shell in the SingleStoreDB server."""
        if platform.platform().lower().startswith('macos'):
            subprocess.call(
                ' '.join([
                    'osascript', '-e',
                    'tell app "Terminal" to do script "' +
                    ' '.join([
                        'mysql', '-h', self._endpoint,
                        '-P', str(self.server_port),
                        '-u', self.user,
                        f'--password=\'{self.password}\'',
                        self._database,
                    ]) +
                    '"',
                ]), shell=True,
            )
        elif platform.platform().lower().startswith('linux'):
            subprocess.call(
                ' '.join([
                    'gnome-terminal', '--',
                    'mysql', '-h', self._endpoint,
                    '-P', str(self.server_port),
                    '-u', self.user,
                    f'--password="{self.password}"',
                    self._database,
                ]), shell=True,
            )
        elif platform.platform().lower().startswith('windows'):
            subprocess.call(
                ' '.join([
                    'start', 'cmd', '/k'
                    'mysql', '-h', self._endpoint,
                    '-P', str(self.server_port),
                    '-u', self.user,
                    f'--password="{self.password}"',
                    self._database,
                ]), shell=True,
            )
        else:
            raise RuntimeError('unsupported platform')

    def open_mongosh(self) -> None:
        """Open a mongosh in the SingleStoreDB server."""
        if not self.kai_enabled:
            raise RuntimeError('kai interface is not enabled')
        if platform.platform().lower().startswith('macos'):
            subprocess.call([
                'osascript', '-e',
                'tell app "Terminal" to do script "' +
                ' '.join(['mongosh', str(self.kai_url)]) +
                '"',
            ])
        elif platform.platform().lower().startswith('linux'):
            subprocess.call([
                'gnome-terminal', '--',
                'mongosh', str(self.kai_url),
            ])
        elif platform.platform().lower().startswith('windows'):
            subprocess.call([
                'start', 'cmd', '/k'
                'mongosh', str(self.kai_url),
            ])
        else:
            raise RuntimeError('unsupported platform')

    def __enter__(self) -> SingleStoreDB:
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> Optional[bool]:
        self.stop()
        return None

    def stop(self, *args: Any) -> None:
        """Stop the SingleStoreDB server."""
        if self._connected is not None:
            self._restore_server_urls()
            try:
                requests.get(
                    'https://shell.singlestore.com/api/terminate',
                    cookies=self._cookies,
                )
            finally:
                self._connected = False


def start() -> SingleStoreDB:
    """
    Manager for SingleStoreDB server running in Docker.

    Returns
    -------
    SingleStoreDB

    """
    return SingleStoreDB()
