#!/usr/bin/env python
"""Pytest plugin"""
import logging
import os
import socket
import subprocess
import time
import uuid
from collections.abc import Iterator
from enum import Enum
from typing import Optional

import pytest

from . import connect
from .connection import Connection
from .connection import Cursor


logger = logging.getLogger(__name__)


# How many times to attempt to connect to the container
STARTUP_CONNECT_ATTEMPTS = 10
# How long to wait between connection attempts
STARTUP_CONNECT_TIMEOUT_SECONDS = 2
# How many times to check if all connections are closed
TEARDOWN_WAIT_ATTEMPTS = 20
# How long to wait between checking connections
TEARDOWN_WAIT_SECONDS = 2


def _find_free_port() -> int:
    """Find a free port by binding to port 0 and getting the assigned port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        s.listen(1)
        return s.getsockname()[1]


class ExecutionMode(Enum):
    SEQUENTIAL = 1
    LEADER = 2
    FOLLOWER = 3


@pytest.fixture(scope='session')
def execution_mode() -> ExecutionMode:
    """Determine the pytest mode for this process"""

    worker = os.environ.get('PYTEST_XDIST_WORKER')
    worker_count = os.environ.get('PYTEST_XDIST_WORKER_COUNT')

    # If we're not in pytest-xdist, the mode is Sequential
    if worker is None or worker_count is None:
        logger.debug('XDIST environment vars not found')
        return ExecutionMode.SEQUENTIAL

    logger.debug(f'PYTEST_XDIST_WORKER == {worker}')
    logger.debug(f'PYTEST_XDIST_WORKER_COUNT == {worker_count}')

    # If we're the only worker, than the mode is Sequential
    if worker_count == '1':
        return ExecutionMode.SEQUENTIAL
    else:
        # The first worker (named "gw0") is the leader
        # if there are multiple workers
        if worker == 'gw0':
            return ExecutionMode.LEADER
        else:
            return ExecutionMode.FOLLOWER


@pytest.fixture(scope='session')
def node_name() -> Iterator[str]:
    """Determine the name of this worker node"""

    worker = os.environ.get('PYTEST_XDIST_WORKER')

    if worker is None:
        logger.debug('XDIST environment vars not found')
        yield 'master'
    else:
        logger.debug(f'PYTEST_XDIST_WORKER == {worker}')
        yield worker


class _TestContainerManager():
    """Manages the setup and teardown of a SingleStoreDB Dev Container

    If SINGLESTOREDB_URL environment variable is set, the manager will use
    the existing server instead of starting a Docker container. This allows
    tests to run against either an existing server or an automatically
    managed Docker container.
    """

    def __init__(self) -> None:
        # Check if SINGLESTOREDB_URL is already set - if so, use existing server
        self.existing_url = os.environ.get('SINGLESTOREDB_URL')
        self.use_existing = self.existing_url is not None

        if self.use_existing:
            logger.info('Using existing SingleStore server from SINGLESTOREDB_URL')
            self.url = self.existing_url
            # No need to initialize Docker-related attributes
            return

        logger.info('SINGLESTOREDB_URL not set, will start Docker container')

        # Generate unique container name using UUID and worker ID
        worker = os.environ.get('PYTEST_XDIST_WORKER', 'master')
        unique_id = uuid.uuid4().hex[:8]
        self.container_name = f'singlestoredb-test-{worker}-{unique_id}'

        self.dev_image_name = 'ghcr.io/singlestore-labs/singlestoredb-dev'

        # Use SINGLESTORE_LICENSE from environment, or empty string as fallback
        # Empty string works for the client SDK
        license = os.environ.get('SINGLESTORE_LICENSE', '')
        if not license:
            logger.info('SINGLESTORE_LICENSE not set, using empty string')

        self.root_password = 'Q8r4D7yXR8oqn'
        self.environment_vars = {
            'SINGLESTORE_LICENSE': license,
            'ROOT_PASSWORD': f"\"{self.root_password}\"",
            'SINGLESTORE_SET_GLOBAL_DEFAULT_PARTITIONS_PER_LEAF': '1',
        }

        # Use dynamic port allocation to avoid conflicts
        self.mysql_port = _find_free_port()
        self.http_port = _find_free_port()
        self.studio_port = _find_free_port()
        self.ports = [
            (self.mysql_port, '3306'),    # External port -> Internal port
            (self.studio_port, '8080'),   # Studio
            (self.http_port, '9000'),     # Data API
        ]

        self.url = f'root:{self.root_password}@127.0.0.1:{self.mysql_port}'

    @property
    def http_connection_url(self) -> Optional[str]:
        """HTTP connection URL for the SingleStoreDB server using Data API."""
        if self.use_existing:
            # If using existing server, HTTP URL not available from manager
            return None
        return (
            f'singlestoredb+http://root:{self.root_password}@'
            f'127.0.0.1:{self.http_port}'
        )

    def _container_exists(self) -> bool:
        """Check if a container with this name already exists."""
        try:
            result = subprocess.run(
                [
                    'docker', 'ps', '-a', '--filter',
                    f'name={self.container_name}',
                    '--format', '{{.Names}}',
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            return self.container_name in result.stdout
        except subprocess.CalledProcessError:
            return False

    def _cleanup_existing_container(self) -> None:
        """Stop and remove any existing container with the same name."""
        if not self._container_exists():
            return

        logger.info(f'Found existing container {self.container_name}, cleaning up')
        try:
            # Try to stop the container (ignore if it's already stopped)
            subprocess.run(
                ['docker', 'stop', self.container_name],
                capture_output=True,
                check=False,
            )
            # Remove the container
            subprocess.run(
                ['docker', 'rm', self.container_name],
                capture_output=True,
                check=True,
            )
            logger.debug(f'Cleaned up existing container {self.container_name}')
        except subprocess.CalledProcessError as e:
            logger.warning(f'Failed to cleanup existing container: {e}')
            # Continue anyway - the unique name should prevent most conflicts

    def start(self) -> None:
        # Clean up any existing container with the same name
        self._cleanup_existing_container()

        command = ' '.join(self._start_command())

        logger.info(
            f'Starting container {self.container_name} on ports {self.mysql_port}, '
            f'{self.http_port}, {self.studio_port}',
        )
        try:
            license = os.environ.get('SINGLESTORE_LICENSE', '')
            env = {
                'SINGLESTORE_LICENSE': license,
            }
            # Capture output to avoid printing the container ID hash
            subprocess.check_call(
                command, shell=True, env=env,
                stdout=subprocess.DEVNULL,
            )

        except Exception as e:
            logger.exception(e)
            raise RuntimeError(
                f'Failed to start container {self.container_name}. '
                f'Command: {command}',
            ) from e
        logger.debug('Container started')

    def _start_command(self) -> Iterator[str]:
        yield 'docker run -d --name'
        yield self.container_name
        for key, value in self.environment_vars.items():
            yield '-e'
            if value is None:
                yield key
            else:
                yield f'{key}={value}'

        for external_port, internal_port in self.ports:
            yield '-p'
            yield f'{external_port}:{internal_port}'

        yield self.dev_image_name

    def print_logs(self) -> None:
        logs_command = ['docker', 'logs', self.container_name]
        logger.info('Getting logs')
        logger.info(subprocess.check_output(logs_command))

    def connect(self) -> Connection:
        # Run all but one attempts trying again if they fail
        for i in range(STARTUP_CONNECT_ATTEMPTS - 1):
            try:
                return connect(self.url)
            except Exception:
                logger.debug(f'Database not available yet (attempt #{i}).')
                time.sleep(STARTUP_CONNECT_TIMEOUT_SECONDS)
        else:
            # Try one last time and report error if it fails
            try:
                return connect(self.url)
            except Exception as e:
                logger.error('Timed out while waiting to connect to database.')
                logger.exception(e)
                self.print_logs()
                raise RuntimeError('Failed to connect to database') from e

    def wait_till_connections_closed(self) -> None:
        heart_beat = connect(self.url)
        for i in range(TEARDOWN_WAIT_ATTEMPTS):
            connections = self.get_open_connections(heart_beat)
            if connections is None:
                raise RuntimeError('Could not determine the number of open connections.')
            logger.debug(
                f'Waiting for other connections (n={connections-1}) '
                f'to close (attempt #{i})',
            )
            time.sleep(TEARDOWN_WAIT_SECONDS)
        else:
            logger.warning('Timed out while waiting for other connections to close')
            self.print_logs()

    def get_open_connections(self, conn: Connection) -> Optional[int]:
        for row in conn.show.status(extended=True):
            name = row['Name']
            value = row['Value']
            logger.info(f'{name} = {value}')
            if name == 'Threads_connected':
                return int(value)

        return None

    def stop(self) -> None:
        logger.info('Cleaning up SingleStore DB dev container')
        logger.debug('Stopping container')
        try:
            subprocess.check_call(
                f'docker stop {self.container_name}',
                shell=True,
                stdout=subprocess.DEVNULL,
            )

        except Exception as e:
            logger.exception(e)
            raise RuntimeError('Failed to stop container.') from e

        logger.debug('Removing container')
        try:
            subprocess.check_call(
                f'docker rm {self.container_name}',
                shell=True,
                stdout=subprocess.DEVNULL,
            )

        except Exception as e:
            logger.exception(e)
            raise RuntimeError('Failed to remove container.') from e


@pytest.fixture(scope='session')
def singlestoredb_test_container(
    execution_mode: ExecutionMode,
) -> Iterator[_TestContainerManager]:
    """Sets up and tears down the test container

    If SINGLESTOREDB_URL is set in the environment, uses the existing server
    and skips Docker container lifecycle management. Otherwise, automatically
    starts a Docker container for testing.
    """

    if not isinstance(execution_mode, ExecutionMode):
        raise TypeError(f"Invalid execution mode '{execution_mode}'")

    container_manager = _TestContainerManager()

    # If using existing server, skip all Docker lifecycle management
    if container_manager.use_existing:
        logger.info('Using existing server, skipping Docker container lifecycle')
        yield container_manager
        return

    # In sequential operation do all the steps
    if execution_mode == ExecutionMode.SEQUENTIAL:
        logger.debug('Not distributed')
        container_manager.start()
        yield container_manager
        container_manager.stop()

    # In distributed execution as leader,
    # do the steps but wait for other workers before stopping
    elif execution_mode == ExecutionMode.LEADER:
        logger.debug('Distributed leader')
        container_manager.start()
        yield container_manager
        container_manager.wait_till_connections_closed()
        container_manager.stop()

    # In distributed exeuction as a non-leader,
    # don't worry about the container lifecycle
    elif execution_mode == ExecutionMode.FOLLOWER:
        logger.debug('Distributed follower')
        yield container_manager


@pytest.fixture(scope='session')
def singlestoredb_connection(
    singlestoredb_test_container: _TestContainerManager,
) -> Iterator[Connection]:
    """Creates and closes the connection"""

    connection = singlestoredb_test_container.connect()
    logger.debug('Connected to database.')

    yield connection

    logger.debug('Closing connection')
    connection.close()


class _NameAllocator():
    """Generates unique names for each database"""

    def __init__(self, id: str) -> None:
        self.id = id
        self.names = 0

    def get_name(self) -> str:
        name = f'x_db_{self.id}_{self.names}'
        self.names += 1
        return name


@pytest.fixture(scope='session')
def name_allocator(node_name: str) -> Iterator[_NameAllocator]:
    """Makes a worker-local name allocator using the node name"""

    yield _NameAllocator(node_name)


@pytest.fixture
def singlestoredb_tempdb(
    singlestoredb_connection: Connection, name_allocator: _NameAllocator,
) -> Iterator[Cursor]:
    """Provides a connection to a unique temporary test database"""

    assert singlestoredb_connection.is_connected(), 'Database is no longer connected'
    db = name_allocator.get_name()

    with singlestoredb_connection.cursor() as cursor:
        logger.debug(f"Creating temporary DB \"{db}\"")
        cursor.execute(f'CREATE DATABASE {db}')
        cursor.execute(f'USE {db}')

        yield cursor

        logger.debug(f"Dropping temporary DB \"{db}\"")
        cursor.execute(f'DROP DATABASE {db}')
