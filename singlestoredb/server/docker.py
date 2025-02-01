#!/usr/bin/env python
"""Utilities for running singlestoredb-dev-image."""
from __future__ import annotations

import atexit
import os
import platform
import secrets
import time
import urllib.parse
from types import TracebackType
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Type

import docker

from .. import connect
from ..connection import Connection

try:
    import pymongo
    has_pymongo = True
except ImportError:
    has_pymongo = False


class SingleStoreDB:

    name: Optional[str]
    hostname: Optional[str]
    root_password: str
    license: str
    kai_enabled: bool
    server_port: int
    studio_port: int
    data_api_port: int
    kai_port: int
    data_dir: Optional[str]
    logs_dir: Optional[str]
    server_dir: Optional[str]
    global_vars: Dict[str, Any]

    def __init__(
        self,
        name: Optional[str] = None,
        root_password: Optional[str] = None,
        license: Optional[str] = None,
        enable_kai: bool = False,
        server_port: int = 3306,
        studio_port: int = 8080,
        data_api_port: int = 9000,
        kai_port: int = 27017,
        hostname: Optional[str] = None,
        data_dir: Optional[str] = None,
        logs_dir: Optional[str] = None,
        server_dir: Optional[str] = None,
        global_vars: Optional[Dict[str, Any]] = None,
        init_sql: Optional[str] = None,
        image: str = 'ghcr.io/singlestore-labs/singlestoredb-dev:latest',
    ):
        self.kai_enabled = enable_kai
        self.server_port = server_port
        self.studio_port = studio_port
        self.data_api_port = data_api_port
        self.kai_port = kai_port
        self.data_dir = data_dir
        self.logs_dir = logs_dir
        self.server_dir = server_dir
        self.hostname = hostname

        # Setup container ports
        ports = {
            '3306/tcp': server_port,
            '8080/tcp': studio_port,
            '9000/tcp': data_api_port,
        }

        if enable_kai:
            ports['27017/tcp'] = kai_port

        # Setup root password
        self.root_password = root_password or secrets.token_urlsafe(10)

        # Setup license value
        if license is None:
            try:
                self.license = os.environ['SINGLESTORE_LICENSE']
            except KeyError:
                raise ValueError('a SingleStore license must be supplied')
        else:
            self.license = license

        env = {
            'ROOT_PASSWORD': self.root_password,
            'SINGLESTORE_LICENSE': self.license,
        }

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

        for pname, pvalue in [('name', name), ('hostname', hostname)]:
            if pvalue is not None:
                kwargs[pname] = pvalue

        # Setup volumes
        volumes: Dict[str, Dict[str, str]] = {}
        if data_dir:
            {data_dir: {'bind': '/data', 'mode': 'rw'}}
        if logs_dir:
            {logs_dir: {'bind': '/logs', 'mode': 'ro'}}
        if server_dir:
            {server_dir: {'bind': '/server', 'mode': 'ro'}}
        if init_sql:
            {init_sql: {'bind': '/init.sql', 'mode': 'ro'}}
        if volumes:
            kwargs['volumes'] = volumes

        # Setup global vars
        self.global_vars = global_vars or {}
        for k, v in self.global_vars.items():
            env['SINGLESTORE_SET_GLOBAL_' + k.upper()] = str(v)

        self._saved_server_urls: Dict[str, Optional[str]] = {}

        docker_client = docker.from_env()
        self.container = docker_client.containers.run(image, **kwargs)
        atexit.register(self.stop)

        if not self._wait_on_ready():
            raise RuntimeError('server did not come up properly')

        self._set_server_urls()

    def _set_server_urls(self) -> None:
        self._saved_server_urls['DATABASE_URL'] = os.environ.get('DATABASE_URL')
        os.environ['DATABASE_URL'] = self.connection_url

    def _restore_server_urls(self) -> None:
        for k, v in self._saved_server_urls.items():
            if v is None:
                del os.environ[k]
            else:
                os.environ[k] = v

    def _wait_on_ready(self) -> bool:
        for i in range(20):
            for line in self.logs():
                if 'INFO: ' in line:
                    return True
            time.sleep(3)
        return False

    def logs(self) -> List[str]:
        return self.container.logs().decode('utf8').split('\n')

    @property
    def connection_url(self) -> str:
        root_password = urllib.parse.quote_plus(self.root_password)
        return f'singlestoredb://root:{root_password}@' + \
               f'localhost:{self.server_port}'

    @property
    def http_connection_url(self) -> str:
        root_password = urllib.parse.quote_plus(self.root_password)
        return f'singlestoredb+http://root:{root_password}@' + \
               f'localhost:{self.data_api_port}'

    def connect(
        self,
        use_data_api: bool = False,
        **kwargs: Any,
    ) -> Connection:
        if use_data_api:
            return connect(self.http_connection_url, **kwargs)
        return connect(self.connection_url, **kwargs)

    @property
    def kai_url(self) -> Optional[str]:
        if not self.kai_enabled:
            return None
        root_password = urllib.parse.quote_plus(self.root_password)
        return f'mongodb://root:{root_password}@' + \
               f'localhost:{self.kai_port}/?authMechanism=PLAIN&loadBalanced=true'

    def connect_kai(self) -> 'pymongo.MongoClient':
        if not self.kai_enabled:
            raise RuntimeError('kai is not enabled')
        if not has_pymongo:
            raise RuntimeError('pymongo is not installed')
        return pymongo.MongoClient(self.kai_url)

    @property
    def studio_url(self) -> str:
        root_password = urllib.parse.quote_plus(self.root_password)
        return f'http://root:{root_password}@localhost:{self.studio_port}'

    def connect_studio(self) -> None:
        import webbrowser
        webbrowser.open(self.studio_url)

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

    def stop(self) -> None:
        if self.container is not None:
            self._restore_server_urls()
            self.container.stop()
            self.container = None


def start(
    name: Optional[str] = None,
    root_password: Optional[str] = None,
    license: Optional[str] = None,
    enable_kai: bool = False,
    server_port: int = 3306,
    studio_port: int = 8080,
    data_api_port: int = 9000,
    kai_port: int = 27017,
    hostname: Optional[str] = None,
    data_dir: Optional[str] = None,
    logs_dir: Optional[str] = None,
    server_dir: Optional[str] = None,
    global_vars: Optional[Dict[str, Any]] = None,
    init_sql: Optional[str] = None,
    image: str = 'ghcr.io/singlestore-labs/singlestoredb-dev:latest',
) -> SingleStoreDB:
    return SingleStoreDB(
        name=name,
        root_password=root_password,
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
    )
