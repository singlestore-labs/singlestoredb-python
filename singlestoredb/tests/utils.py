#!/usr/bin/env python
# type: ignore
"""Utilities for testing."""
import glob
import logging
import os
import re
import uuid
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from urllib.parse import urlparse

import singlestoredb as s2
from singlestoredb.connection import build_params


logger = logging.getLogger(__name__)


def apply_template(content: str, vars: Dict[str, Any]) -> str:
    for k, v in vars.items():
        key = '{{%s}}' % k
        if key in content:
            content = content.replace(key, v)
    return content


def get_server_version(cursor: Any) -> Tuple[int, int]:
    """
    Get the server version as a (major, minor) tuple.

    Parameters
    ----------
    cursor : Cursor
        Database cursor to execute queries

    Returns
    -------
    (int, int)
        Tuple of (major_version, minor_version)
    """
    cursor.execute('SELECT @@memsql_version')
    version_str = cursor.fetchone()[0]
    # Parse version string like "9.1.2" or "9.1.2-abc123"
    version_parts = version_str.split('-')[0].split('.')
    major = int(version_parts[0])
    minor = int(version_parts[1]) if len(version_parts) > 1 else 0
    logger.info(f'Detected server version: {major}.{minor} (full: {version_str})')
    return (major, minor)


def find_version_specific_sql_files(base_dir: str) -> List[Tuple[int, int, str]]:
    """
    Find all version-specific SQL files in the given directory.

    Looks for files matching the pattern test_X_Y.sql where X is major
    version and Y is minor version.

    Parameters
    ----------
    base_dir : str
        Directory to search for SQL files

    Returns
    -------
    List[Tuple[int, int, str]]
        List of (major, minor, filepath) tuples sorted by version
    """
    pattern = os.path.join(base_dir, 'test_*_*.sql')
    files = []

    for filepath in glob.glob(pattern):
        filename = os.path.basename(filepath)
        # Match pattern: test_X_Y.sql
        match = re.match(r'test_(\d+)_(\d+)\.sql$', filename)
        if match:
            major = int(match.group(1))
            minor = int(match.group(2))
            files.append((major, minor, filepath))
            logger.debug(
                f'Found version-specific SQL file: {filename} '
                f'(v{major}.{minor})',
            )

    # Sort by version (major, minor)
    files.sort()
    return files


def load_version_specific_sql(
    cursor: Any,
    base_dir: str,
    server_version: Tuple[int, int],
    template_vars: Dict[str, Any],
) -> None:
    """
    Load version-specific SQL files based on server version.

    Parameters
    ----------
    cursor : Cursor
        Database cursor to execute queries
    base_dir : str
        Directory containing SQL files
    server_version : Tuple[int, int]
        Server version as (major, minor)
    template_vars : Dict[str, Any]
        Template variables to apply to SQL content
    """
    sql_files = find_version_specific_sql_files(base_dir)
    server_major, server_minor = server_version

    for file_major, file_minor, filepath in sql_files:
        # Load if server version >= file version
        if (
            server_major > file_major or
            (server_major == file_major and server_minor >= file_minor)
        ):
            logger.info(
                f'Loading version-specific SQL: {os.path.basename(filepath)} '
                f'(requires {file_major}.{file_minor}, '
                f'server is {server_major}.{server_minor})',
            )
            with open(filepath, 'r') as sql_file:
                for cmd in sql_file.read().split(';\n'):
                    cmd = apply_template(cmd.strip(), template_vars)
                    if cmd:
                        cmd += ';'
                        cursor.execute(cmd)
        else:
            logger.info(
                f'Skipping version-specific SQL: {os.path.basename(filepath)} '
                f'(requires {file_major}.{file_minor}, '
                f'server is {server_major}.{server_minor})',
            )


def load_sql(sql_file: str) -> str:
    """
    Load a file containing SQL code.

    Parameters
    ----------
    sql_file : str
        Name of the SQL file to load.

    Returns
    -------
    (str, bool)
        Name of database created for SQL file and a boolean indicating
        whether the database already existed (meaning that it should not
        be deleted when tests are finished).

    """
    dbname = None

    # Use an existing database name if given.
    if 'SINGLESTOREDB_URL' in os.environ:
        dbname = build_params(host=os.environ['SINGLESTOREDB_URL']).get('database')
    elif 'SINGLESTOREDB_HOST' in os.environ:
        dbname = build_params(host=os.environ['SINGLESTOREDB_HOST']).get('database')
    elif 'SINGLESTOREDB_DATABASE' in os.environ:
        dbname = os.environ['SINGLESTOREDB_DATBASE']

    # Use initializer URL if given for setup operations.
    # HTTP can't change databases or execute certain commands like SET GLOBAL,
    # so we always use the MySQL protocol URL for initialization.
    args = {'local_infile': True}
    if 'SINGLESTOREDB_INIT_DB_URL' in os.environ:
        args['host'] = os.environ['SINGLESTOREDB_INIT_DB_URL']
        logger.info(
            f'load_sql: Using SINGLESTOREDB_INIT_DB_URL for setup: '
            f'{os.environ["SINGLESTOREDB_INIT_DB_URL"]}',
        )

    http_port = 0
    if 'SINGLESTOREDB_URL' in os.environ:
        url = os.environ['SINGLESTOREDB_URL']
        if url.startswith('http:') or url.startswith('https:'):
            urlp = urlparse(url)
            if urlp.port:
                http_port = urlp.port

    if 'SINGLESTOREDB_HTTP_PORT' in os.environ:
        http_port = int(os.environ['SINGLESTOREDB_HTTP_PORT'])

    dbexisted = bool(dbname)

    template_vars = dict(DATABASE_NAME=dbname, TEST_PATH=os.path.dirname(sql_file))

    # Always use the default driver since not all operations are
    # permitted in the HTTP API.
    with open(sql_file, 'r') as infile:
        with s2.connect(**args) as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute('SET GLOBAL default_partitions_per_leaf=2')
                    cur.execute('SET GLOBAL log_file_size_partitions=1048576')
                    cur.execute('SET GLOBAL log_file_size_ref_dbs=1048576')
                except s2.OperationalError:
                    pass

                if not dbname:
                    dbname = 'TEST_{}'.format(uuid.uuid4()).replace('-', '_')
                    cur.execute(f'CREATE DATABASE {dbname};')
                    cur.execute(f'USE {dbname};')

                    template_vars['DATABASE_NAME'] = dbname

                    # Execute lines in SQL.
                    for cmd in infile.read().split(';\n'):
                        cmd = apply_template(cmd.strip(), template_vars)
                        if cmd:
                            cmd += ';'
                            cur.execute(cmd)

                elif not conn.driver.startswith('http'):
                    cur.execute(f'USE {dbname};')

                # Start HTTP server as needed.
                if http_port and not conn.driver.startswith('http'):
                    cur.execute(f'SET GLOBAL HTTP_PROXY_PORT={http_port};')
                    cur.execute('SET GLOBAL HTTP_API=ON;')
                    cur.execute('RESTART PROXY;')

                # Load version-specific SQL files (e.g., test_9_1.sql for 9.1+)
                try:
                    server_version = get_server_version(cur)
                    sql_dir = os.path.dirname(sql_file)
                    load_version_specific_sql(
                        cur,
                        sql_dir,
                        server_version,
                        template_vars,
                    )
                except Exception as e:
                    logger.warning(
                        f'Failed to load version-specific SQL files: {e}',
                    )

    return dbname, dbexisted


def drop_database(name: str) -> None:
    """Drop a database with the given name."""
    if name:
        args = {}
        if 'SINGLESTOREDB_INIT_DB_URL' in os.environ:
            args['host'] = os.environ['SINGLESTOREDB_INIT_DB_URL']
        with s2.connect(**args) as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP DATABASE {name};')


def create_user(name: str, password: str, dbname: str) -> None:
    """Create a user for the test database."""
    if name:
        args = {}
        if 'SINGLESTOREDB_INIT_DB_URL' in os.environ:
            args['host'] = os.environ['SINGLESTOREDB_INIT_DB_URL']
        with s2.connect(**args) as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP USER IF EXISTS {name};')
                cur.execute(f'CREATE USER "{name}"@"%" IDENTIFIED BY "{password}"')
                cur.execute(f'GRANT ALL ON {dbname}.* to "{name}"@"%"')


def drop_user(name: str) -> None:
    """Drop a database with the given name."""
    if name:
        args = {}
        if 'SINGLESTOREDB_INIT_DB_URL' in os.environ:
            args['host'] = os.environ['SINGLESTOREDB_INIT_DB_URL']
        with s2.connect(**args) as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP USER IF EXISTS {name};')


#
# Live deployment tracking
#
# Every workspace group, workspace, cluster and starter cluster a test creates
# costs money until it is terminated, and the usual `tearDownClass` is not
# enough on its own:
#
#   * unittest does not call `tearDownClass` at all if `setUpClass` raises, so
#     a fixture that dies partway through -- two of three clusters created,
#     then a dropped connection -- leaks everything it had made so far;
#   * a test that creates a deployment in its body and then fails before its
#     own cleanup line leaks it too.
#
# So creations are registered here as well, and `cleanup_tracked()` sweeps
# whatever is left: per test class as the run moves on to the next one, and
# again for everything at the end of the session (see conftest.py).
# Terminating twice is harmless -- the second attempt finds it gone and is
# ignored -- so tracked objects do not have to be untracked by the tests that
# clean up after themselves.
#

#: (owner, label, object) for every deployment created so far and not yet
#: swept. The owner is the test class that was running at creation time, so
#: a class's leftovers can be dropped when the run leaves that class rather
#: than idling -- and billing -- until the session ends.
_tracked: List[Tuple[str, str, Any]] = []

#: Test class currently running, as set by conftest.
_owner = ''


def get_owner() -> str:
    """Return the test class creations are currently attributed to."""
    return _owner


def set_owner(owner: str) -> None:
    """Record which test class subsequent creations belong to."""
    global _owner
    _owner = owner


def _is_mocked(obj: Any) -> bool:
    """
    Did this object come out of a mocked manager?

    The unit tests call the same creation methods with ``_post`` patched, and
    the objects they get back name deployments that do not exist. Sweeping
    those would be a round trip per fake object and a warning apiece.
    """
    from unittest.mock import NonCallableMock

    if isinstance(obj, NonCallableMock):
        return True
    manager = getattr(obj, '_manager', None)
    if manager is None:
        return True
    if isinstance(manager, NonCallableMock):
        return True
    return any(
        isinstance(getattr(manager, x, None), NonCallableMock)
        for x in ('_get', '_post', '_delete')
    )


def track(obj: Any, label: str = '') -> Any:
    """
    Register a live deployment for end-of-session cleanup.

    Returns the object, so it can wrap a creation call in place::

        cls.cluster = utils.track(mgr.create_cluster(...))

    """
    if obj is not None and not _is_mocked(obj):
        _tracked.append((
            _owner,
            label or '{} {!r}'.format(
                type(obj).__name__, getattr(obj, 'name', None) or
                getattr(obj, 'id', '?'),
            ),
            obj,
        ))
    return obj


def untrack(obj: Any) -> None:
    """Forget a deployment that has been terminated."""
    for i, entry in reversed(list(enumerate(_tracked))):
        if entry[2] is obj:
            _tracked.pop(i)


def terminate(obj: Any) -> None:
    """
    Terminate a deployment, whatever kind it is.

    ``force=True`` is what makes a workspace group with live workspaces in it
    go away; the starter variants take no arguments at all.
    """
    try:
        obj.terminate(force=True)
    except TypeError:
        obj.terminate()


#: (module, class, method) triples that bring a billable deployment into
#: existence. Wrapping them is what makes tracking automatic, so a new test
#: cannot leak a cluster by forgetting to register it.
_CREATORS = [
    (
        'singlestoredb.management.v1.workspace', 'WorkspaceManager',
        'create_workspace_group',
    ),
    (
        'singlestoredb.management.v1.workspace', 'WorkspaceManager',
        'create_workspace',
    ),
    (
        'singlestoredb.management.v1.workspace', 'WorkspaceManager',
        'create_starter_workspace',
    ),
    (
        'singlestoredb.management.v1.workspace', 'WorkspaceGroup',
        'create_workspace',
    ),
    ('singlestoredb.management.v2.cluster', 'ClusterManager', 'create_cluster'),
    (
        'singlestoredb.management.v2.cluster', 'ClusterManager',
        'create_starter_cluster',
    ),
]

_tracking_installed = False


def install_deployment_tracking() -> None:
    """
    Wrap the deployment creation methods so their results are tracked.

    Called from ``pytest_configure`` rather than a fixture: it has to be in
    place before any test module is imported, since a ``setUpClass`` can run
    creations that a later fixture would never see.
    """
    global _tracking_installed
    if _tracking_installed:
        return
    _tracking_installed = True

    import functools
    import importlib

    def wrap(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return track(func(*args, **kwargs))
        return wrapper

    for module_name, class_name, method_name in _CREATORS:
        try:
            klass = getattr(importlib.import_module(module_name), class_name)
            setattr(klass, method_name, wrap(getattr(klass, method_name)))
        except AttributeError as exc:
            # A renamed method must not silently stop being tracked.
            logger.warning(
                f'Cannot track {module_name}.{class_name}.'
                f'{method_name}: {exc}',
            )


def _is_gone(obj: Any) -> bool:
    """
    Has this deployment already been terminated?

    The local copy is stale -- a test that terminated in its own teardown
    still holds an object whose ``terminated_at`` is None -- so ask the
    server. A refresh that fails is taken as gone, which is the whole point
    of the question for anything that 404s.
    """
    if hasattr(obj, 'refresh'):
        try:
            obj.refresh()
        except Exception:
            return True
    if getattr(obj, 'terminated_at', None) is not None:
        return True
    return str(getattr(obj, 'state', '') or '').upper() in (
        'TERMINATED', 'TERMINATING',
    )


def cleanup_tracked(owner: Optional[str] = None) -> List[str]:
    """
    Terminate tracked deployments that are still live.

    Parameters
    ----------
    owner : str, optional
        Only sweep what this test class created. The default sweeps
        everything, which is what the end of the session wants.

    Returns
    -------
    List[str]
        Labels of the deployments this call terminated. Failures are logged
        rather than raised: this runs outside any test, where an exception
        would be reported against whatever happens to run next.

    """
    # Last created, first terminated: a workspace goes before the group that
    # holds it.
    entries = [x for x in reversed(_tracked) if owner is None or x[0] == owner]
    for entry in entries:
        _tracked.remove(entry)

    removed = []
    for _, label, obj in entries:
        if _is_gone(obj):
            continue
        try:
            terminate(obj)
        except Exception as exc:
            logger.warning(f'Could not terminate {label}: {exc}')
        else:
            removed.append(label)
    return removed
