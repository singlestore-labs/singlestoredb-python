#!/usr/bin/env python
"""Pytest configuration for singlestoredb tests

This module sets up automatic Docker container management for tests.
It works with both pytest-style and unittest-style tests.

The conftest automatically:
1. Checks if SINGLESTOREDB_URL is set in the environment
2. If not set, starts a SingleStore Docker container
3. Sets SINGLESTOREDB_URL for all tests to use
4. Cleans up the container when tests complete

Environment Variables:
    - SINGLESTOREDB_URL: If set, tests will use this existing server instead
                        of starting a Docker container. This allows testing
                        against a specific server instance.
    - USE_DATA_API: If set to 1/true/on, tests will use HTTP Data API
                   instead of MySQL protocol. When set, SINGLESTOREDB_URL
                   will be set to the HTTP URL, and SINGLESTOREDB_INIT_DB_URL
                   will be set to the MySQL URL for setup operations.
    - SINGLESTORE_LICENSE: Optional. License key for Docker container. If not
                          set, an empty string is used as fallback.

Available Fixtures:
    - singlestoredb_test_container: Manages Docker container lifecycle
    - singlestoredb_connection: Provides a connection to the test server
    - singlestoredb_tempdb: Creates a temporary test database with cursor
"""
import logging
import os
from collections.abc import Iterator
from typing import Optional

import pytest

from singlestoredb.pytest import _TestContainerManager
from singlestoredb.pytest import execution_mode  # noqa: F401
from singlestoredb.pytest import ExecutionMode  # noqa: F401
from singlestoredb.pytest import name_allocator  # noqa: F401
from singlestoredb.pytest import node_name  # noqa: F401
from singlestoredb.pytest import singlestoredb_connection  # noqa: F401
from singlestoredb.pytest import singlestoredb_tempdb  # noqa: F401


logger = logging.getLogger(__name__)

# Global container manager instance
_container_manager: Optional[_TestContainerManager] = None


def pytest_configure(config: pytest.Config) -> None:
    """
    Pytest hook that runs before test collection.

    This ensures the Docker container is started (if needed) before any
    test modules are imported. Some test modules try to get connection
    parameters at import time, so we need the environment set up early.
    """
    global _container_manager

    # Prevent double initialization - pytest_configure can be called multiple times
    if _container_manager is not None:
        logger.debug('pytest_configure already called, skipping')
        return

    if 'SINGLESTOREDB_URL' not in os.environ:
        print('\n' + '=' * 70)
        print('Starting SingleStoreDB Docker container...')
        print('This may take a moment...')
        print('=' * 70)
        logger.info('SINGLESTOREDB_URL not set, starting Docker container')

        # Create and start the container
        _container_manager = _TestContainerManager()

        if not _container_manager.use_existing:
            _container_manager.start()
            print(f'Container {_container_manager.container_name} started')
            print('Waiting for SingleStoreDB to be ready...')

            # Wait for container to be ready
            try:
                conn = _container_manager.connect()
                conn.close()
                print('✓ SingleStoreDB is ready!')
                logger.info('Docker container is ready')
            except Exception as e:
                print(f'✗ Failed to connect to Docker container: {e}')
                logger.error(f'Failed to connect to Docker container: {e}')
                raise

        # Set the environment variable for all tests
        # Check if USE_DATA_API is set to use HTTP connection
        if os.environ.get('USE_DATA_API', '0').lower() in ('1', 'true', 'on'):
            # Use HTTP URL for tests
            url = _container_manager.http_connection_url
            if url is None:
                raise RuntimeError(
                    'Failed to get HTTP URL from container manager',
                )
            os.environ['SINGLESTOREDB_URL'] = url
            print('=' * 70)
            print('USE_DATA_API is enabled - using HTTP Data API for tests')
            print(f'Tests will connect via: {url}')
            print('=' * 70)
            logger.info('USE_DATA_API is enabled - using HTTP Data API for tests')
            logger.info(f'Tests will connect via: {url}')

            # Also set INIT_DB_URL to MySQL URL for setup operations
            # (like SET GLOBAL) that don't work over HTTP
            mysql_url = _container_manager.url
            if mysql_url is None:
                raise RuntimeError(
                    'Failed to get MySQL URL from container manager',
                )
            os.environ['SINGLESTOREDB_INIT_DB_URL'] = mysql_url
            print(f'Setup operations will use MySQL protocol: {mysql_url}')
            logger.info(
                f'Setup operations will use MySQL protocol: {mysql_url}',
            )
        else:
            url = _container_manager.url
            if url is None:
                raise RuntimeError(
                    'Failed to get database URL from container manager',
                )
            os.environ['SINGLESTOREDB_URL'] = url
            print('=' * 70)
            print(f'Tests will connect via MySQL protocol: {url}')
            print('=' * 70)
            logger.info(f'Tests will connect via MySQL protocol: {url}')
    else:
        url = os.environ['SINGLESTOREDB_URL']
        logger.debug(f'Using existing SINGLESTOREDB_URL={url}')


def pytest_unconfigure(config: pytest.Config) -> None:
    """
    Pytest hook that runs after all tests complete.

    Cleans up the Docker container if one was started.
    """
    global _container_manager

    if _container_manager is not None and not _container_manager.use_existing:
        print('\n' + '=' * 70)
        print('Cleaning up Docker container...')
        logger.info('Cleaning up Docker container')
        try:
            _container_manager.stop()
            print(f'✓ Container {_container_manager.container_name} stopped')
            print('=' * 70)
            logger.info('Docker container stopped')
        except Exception as e:
            print(f'✗ Failed to stop Docker container: {e}')
            print('=' * 70)
            logger.error(f'Failed to stop Docker container: {e}')


@pytest.fixture(scope='session', autouse=True)
def setup_test_environment() -> Iterator[None]:
    """
    Automatically set up test environment for all tests.

    This fixture ensures the test environment is ready. The actual container
    setup happens in pytest_configure hook to ensure it runs before test
    collection. Cleanup happens in pytest_unconfigure hook.

    This fixture exists to ensure proper ordering but doesn't manage the
    container lifecycle itself.
    """
    # The environment should already be set up by pytest_configure
    # This fixture just ensures proper test initialization order
    yield

    # Clean up is handled by pytest_unconfigure


@pytest.fixture(autouse=True)
def protect_singlestoredb_url() -> Iterator[None]:
    """
    Protect SINGLESTOREDB_URL and SINGLESTOREDB_INIT_DB_URL from corruption.

    Some tests (like test_config.py) call reset_option() which resets all
    config options to their defaults. Since the 'host' option is registered
    with environ=['SINGLESTOREDB_HOST', 'SINGLESTOREDB_URL'], resetting it
    overwrites SINGLESTOREDB_URL with just '127.0.0.1' instead of the full
    connection string, breaking subsequent tests.

    This fixture saves both URLs before each test and restores them
    after, ensuring they're not corrupted.
    """
    # Save the current URLs
    saved_url = os.environ.get('SINGLESTOREDB_URL')
    saved_init_url = os.environ.get('SINGLESTOREDB_INIT_DB_URL')

    yield

    # Restore SINGLESTOREDB_URL if it was set and has been corrupted
    if saved_url is not None:
        current_url = os.environ.get('SINGLESTOREDB_URL')
        if current_url != saved_url:
            logger.debug(
                f'Restoring SINGLESTOREDB_URL from {current_url!r} to {saved_url!r}',
            )
            os.environ['SINGLESTOREDB_URL'] = saved_url

    # Restore SINGLESTOREDB_INIT_DB_URL if it was set and has been corrupted
    if saved_init_url is not None:
        current_init_url = os.environ.get('SINGLESTOREDB_INIT_DB_URL')
        if current_init_url != saved_init_url:
            logger.debug(
                f'Restoring SINGLESTOREDB_INIT_DB_URL from '
                f'{current_init_url!r} to {saved_init_url!r}',
            )
            os.environ['SINGLESTOREDB_INIT_DB_URL'] = saved_init_url
