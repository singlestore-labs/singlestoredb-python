#!/usr/bin/env python
"""Pytest plugin"""
import logging
import os
import subprocess
import time
from enum import Enum
from typing import Iterator
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
    """Manages the setup and teardown of a SingleStoreDB Dev Container"""

    def __init__(self) -> None:
        self.container_name = 'singlestoredb-test-container'
        self.dev_image_name = 'ghcr.io/singlestore-labs/singlestoredb-dev'

        assert 'SINGLESTORE_LICENSE' in os.environ, 'SINGLESTORE_LICENSE not set'

        self.root_password = 'Q8r4D7yXR8oqn'
        self.environment_vars = {
            'SINGLESTORE_LICENSE': None,
            'ROOT_PASSWORD': f"\"{self.root_password}\"",
            'SINGLESTORE_SET_GLOBAL_DEFAULT_PARTITIONS_PER_LEAF': '1',
        }

        self.ports = ['3306', '8080', '9000']

        self.url = f'root:{self.root_password}@127.0.0.1:3306'

    def start(self) -> None:
        command = ' '.join(self._start_command())

        logger.info(f'Starting container {self.container_name}')
        try:
            license = os.environ['SINGLESTORE_LICENSE']
            env = {
                'SINGLESTORE_LICENSE': license,
            }
            subprocess.check_call(command, shell=True, env=env)
        except Exception as e:
            logger.exception(e)
            raise RuntimeError(
                'Failed to start container. '
                'Is one already running?',
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

        for port in self.ports:
            yield '-p'
            yield f'{port}:{port}'

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
            subprocess.check_call(f'docker stop {self.container_name}', shell=True)
        except Exception as e:
            logger.exception(e)
            raise RuntimeError('Failed to stop container.') from e

        logger.debug('Removing container')
        try:
            subprocess.check_call(f'docker rm {self.container_name}', shell=True)
        except Exception as e:
            logger.exception(e)
            raise RuntimeError('Failed to stop container.') from e


@pytest.fixture(scope='session')
def singlestoredb_test_container(
    execution_mode: ExecutionMode,
) -> Iterator[_TestContainerManager]:
    """Sets up and tears down the test container"""

    if not isinstance(execution_mode, ExecutionMode):
        raise TypeError(f"Invalid execution mode '{execution_mode}'")

    container_manager = _TestContainerManager()

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
