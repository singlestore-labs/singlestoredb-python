#!/usr/bin/env python
"""Utilities for running singlestoredb-dev Docker image."""
from __future__ import annotations

import atexit
import os
import platform
import secrets
import signal
import socket
import subprocess
import time
import urllib.parse
from contextlib import closing
from types import TracebackType
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Type

try:
    import docker
    has_docker = True
except ImportError:
    has_docker = False
    raise RuntimeError('docker python package is not installed')

from .. import connect
from ..connection import Connection

try:
    import pymongo
    has_pymongo = True
except ImportError:
    has_pymongo = False

DEFAULT_IMAGE = 'ghcr.io/singlestore-labs/singlestoredb-dev:latest'


class SingleStoreDB:
    """
    Manager for SingleStoreDB server running in Docker.

    Parameters
    -----------
    name : str, optional
        Name of the container.
    password : str, optional
        Password for the SingleStoreDB server.
    license : str, optional
        License key for SingleStoreDB.
    enable_kai : bool, optional
        Enable Kai (MongoDB) server.
    server_port : int, optional
        Port for the SingleStoreDB server.
    studio_port : int, optional
        Port for the SingleStoreDB Studio.
    data_api_port : int, optional
        Port for the SingleStoreDB Data API.
    kai_port : int, optional
        Port for the Kai server.
    hostname : str, optional
        Hostname for the container.
    data_dir : str, optional
        Path to the data directory.
    logs_dir : str, optional
        Path to the logs directory.
    server_dir : str, optional
        Path to the server directory.
    global_vars : dict, optional
        Global variables to set in the SingleStoreDB server.
    init_sql : str, optional
        Path to an SQL file to run on startup.
    image : str, optional
        Docker image to use.
    database : str, optional
        Default database to connect to.

    """

    user: str
    password: str
    kai_enabled: bool
    server_port: int
    studio_port: int
    data_api_port: int
    kai_port: Optional[int]
    data_dir: Optional[str]
    logs_dir: Optional[str]
    server_dir: Optional[str]

    def __init__(
        self,
        name: Optional[str] = None,
        *,
        password: Optional[str] = None,
        license: Optional[str] = None,
        enable_kai: bool = False,
        server_port: Optional[int] = None,
        studio_port: Optional[int] = None,
        data_api_port: Optional[int] = None,
        kai_port: Optional[int] = None,
        hostname: Optional[str] = None,
        data_dir: Optional[str] = None,
        logs_dir: Optional[str] = None,
        server_dir: Optional[str] = None,
        global_vars: Optional[Dict[str, Any]] = None,
        init_sql: Optional[str] = None,
        image: str = DEFAULT_IMAGE,
        database: Optional[str] = None,
    ):
        self.kai_enabled = enable_kai
        self.kai_port = None
        self.server_port = server_port or self._get_available_port()
        self.studio_port = studio_port or self._get_available_port()
        self.data_api_port = data_api_port or self._get_available_port()
        self.data_dir = data_dir
        self.logs_dir = logs_dir
        self.server_dir = server_dir
        self.user = 'root'

        # Setup container ports
        ports = {
            '3306/tcp': self.server_port,
            '8080/tcp': self.studio_port,
            '9000/tcp': self.data_api_port,
        }

        if enable_kai:
            self.kai_port = kai_port or self._get_available_port()
            ports['27017/tcp'] = self.kai_port

        # Setup password
        self.password = password or secrets.token_urlsafe(10)

        # Setup license value
        if license is None:
            try:
                license = os.environ['SINGLESTORE_LICENSE']
            except KeyError:
                raise ValueError('a SingleStore license must be supplied')

        # Setup environment variables for the container
        env = {'ROOT_PASSWORD': self.password}

        if license:
            env['SINGLESTORE_LICENSE'] = license

        if enable_kai:
            env['ENABLE_KAI'] = '1'

        # Construct Docker arguments
        kwargs = {
            'environment': env,
            'ports': ports,
            'detach': True,
            'auto_remove': True,
            'remove': True,
        }

        if 'macOS' in platform.platform():
            kwargs['platform'] = 'linux/amd64'

        for pname, pvalue in [
            ('name', name),
            ('hostname', hostname),
        ]:
            if pvalue is not None:
                kwargs[pname] = pvalue

        # Setup volumes
        volumes: Dict[str, Dict[str, str]] = {}
        if data_dir:
            volumes[data_dir] = {'bind': '/data', 'mode': 'rw'}
        if logs_dir:
            volumes[logs_dir] = {'bind': '/logs', 'mode': 'ro'}
        if server_dir:
            volumes[server_dir] = {'bind': '/server', 'mode': 'ro'}
        if init_sql:
            volumes[os.path.abspath(init_sql)] = {'bind': '/init.sql', 'mode': 'ro'}
        if volumes:
            kwargs['volumes'] = volumes

        # Setup global vars
        for k, v in (global_vars or {}).items():
            env['SINGLESTORE_SET_GLOBAL_' + k.upper()] = str(v)

        self._saved_server_urls: Dict[str, Optional[str]] = {}

        docker_client = docker.from_env()
        self.container = docker_client.containers.run(image, **kwargs)

        # Make sure container gets cleaned up at exit
        atexit.register(self.stop)
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

        if not self._wait_on_ready():
            raise RuntimeError('server did not come up properly')

        self._database = database
        self._set_server_urls()

    def __str__(self) -> str:
        return f"SingleStoreDB('{self.connection_url}')"

    def __repr__(self) -> str:
        return str(self)

    def _get_available_port(self) -> int:
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(('', 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

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

    def _wait_on_ready(self) -> bool:
        for i in range(80):
            for line in self.logs():
                if 'INFO: ' in line:
                    return True
            time.sleep(3)
        return False

    def logs(self) -> List[str]:
        return self.container.logs().decode('utf8').split('\n')

    @property
    def connection_url(self) -> str:
        """Connection URL for the SingleStoreDB server."""
        dbname = f'/{self._database}' if self._database else ''
        password = urllib.parse.quote_plus(self.password)
        return f'singlestoredb://{self.user}:{password}@' + \
               f'localhost:{self.server_port}{dbname}'

    @property
    def http_connection_url(self) -> str:
        """HTTP Connection URL for the SingleStoreDB server."""
        dbname = f'/{self._database}' if self._database else ''
        password = urllib.parse.quote_plus(self.password)
        return f'singlestoredb+http://{self.user}:{password}@' + \
               f'localhost:{self.data_api_port}{dbname}'

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
        return f'mongodb://{self.user}:{password}@' + \
               f'localhost:{self.kai_port}/?authMechanism=PLAIN&loadBalanced=true'

    def connect_kai(self) -> 'pymongo.MongoClient':
        """Connect to the Kai (MongoDB) server."""
        if not self.kai_enabled:
            raise RuntimeError('kai is not enabled')
        if not has_pymongo:
            raise RuntimeError('pymongo is not installed')
        return pymongo.MongoClient(self.kai_url)

    @property
    def studio_url(self) -> str:
        """URL for the SingleStoreDB Studio."""
        return f'http://localhost:{self.studio_port}'

    def open_studio(self) -> None:
        """Open the SingleStoreDB Studio in a web browser."""
        import webbrowser
        if platform.platform().lower().startswith('macos'):
            chrome_path = r'open -a /Applications/Google\ Chrome.app %s'
            webbrowser.get(chrome_path).open(self.studio_url, new=2)
        else:
            webbrowser.open(self.studio_url, new=2)

    def open_shell(self) -> None:
        """Open a shell in the SingleStoreDB server."""
        if platform.platform().lower().startswith('macos'):
            subprocess.call([
                'osascript', '-e',
                'tell app "Terminal" to do script '
                f'"docker exec -it {self.container.id} singlestore-auth"',
            ])
        elif platform.platform().lower().startswith('linux'):
            subprocess.call([
                'gnome-terminal', '--',
                'docker', 'exec', '-it', self.container.id, 'singlestore-auth',
            ])
        elif platform.platform().lower().startswith('windows'):
            subprocess.call([
                'start', 'cmd', '/k'
                'docker', 'exec', '-it', self.container.id, 'singlestore-auth',
            ])
        else:
            raise RuntimeError('unsupported platform')

    def open_mongosh(self) -> None:
        """Open a mongosh in the SingleStoreDB server."""
        if not self.kai_enabled:
            raise RuntimeError('kai interface is not enabled')
        if platform.platform().lower().startswith('macos'):
            subprocess.call([
                'osascript', '-e',
                'tell app "Terminal" to do script '
                f'"docker exec -it {self.container.id} mongosh-auth"',
            ])
        elif platform.platform().lower().startswith('linux'):
            subprocess.call([
                'gnome-terminal', '--',
                'docker', 'exec', '-it', self.container.id, 'mongosh-auth',
            ])
        elif platform.platform().lower().startswith('windows'):
            subprocess.call([
                'start', 'cmd', '/k'
                'docker', 'exec', '-it', self.container.id, 'mongosh-auth',
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
        if self.container is not None:
            self._restore_server_urls()
            try:
                self.container.stop()
            finally:
                self.container = None


def start(
    name: Optional[str] = None,
    password: Optional[str] = None,
    license: Optional[str] = None,
    enable_kai: bool = False,
    server_port: Optional[int] = None,
    studio_port: Optional[int] = None,
    data_api_port: Optional[int] = None,
    kai_port: Optional[int] = None,
    hostname: Optional[str] = None,
    data_dir: Optional[str] = None,
    logs_dir: Optional[str] = None,
    server_dir: Optional[str] = None,
    global_vars: Optional[Dict[str, Any]] = None,
    init_sql: Optional[str] = None,
    image: str = DEFAULT_IMAGE,
    database: Optional[str] = None,
) -> SingleStoreDB:
    """
    Manager for SingleStoreDB server running in Docker.

    Parameters
    -----------
    name : str, optional
        Name of the container.
    password : str, optional
        Password for the SingleStoreDB server.
    license : str, optional
        License key for SingleStoreDB.
    enable_kai : bool, optional
        Enable Kai (MongoDB) server.
    server_port : int, optional
        Port for the SingleStoreDB server.
    studio_port : int, optional
        Port for the SingleStoreDB Studio.
    data_api_port : int, optional
        Port for the SingleStoreDB Data API.
    kai_port : int, optional
        Port for the Kai server.
    hostname : str, optional
        Hostname for the container.
    data_dir : str, optional
        Path to the data directory.
    logs_dir : str, optional
        Path to the logs directory.
    server_dir : str, optional
        Path to the server directory.
    global_vars : dict, optional
        Global variables to set in the SingleStoreDB server.
    init_sql : str, optional
        Path to an SQL file to run on startup.
    image : str, optional
        Docker image to use.
    database : str, optional
        Default database to connect to.

    """
    return SingleStoreDB(
        name=name,
        password=password,
        license=license,
        enable_kai=enable_kai,
        server_port=server_port,
        studio_port=studio_port,
        data_api_port=data_api_port,
        kai_port=kai_port,
        hostname=hostname,
        data_dir=data_dir,
        logs_dir=logs_dir,
        server_dir=server_dir,
        global_vars=global_vars,
        init_sql=init_sql,
        image=image,
        database=database,
    )
