#!/usr/bin/env python
# type: ignore
"""Utilities for testing."""
import glob
import logging
import os
import random
import re
import secrets
import unittest
import uuid
from types import SimpleNamespace
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from unittest import mock
from urllib.parse import urlparse

import singlestoredb as s2
from singlestoredb.connection import build_params
from singlestoredb.exceptions import ManagementError
from singlestoredb.management.v2.cluster import ClusterManager as _ClusterManager


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

#: (receiver, finder, args, kwargs) for every creation call currently
#: executing. A creator POSTs and only then waits for the deployment to come
#: up, so for the whole ``wait_on_active`` window -- twenty minutes for a
#: cluster -- something billable exists that nothing has registered yet:
#: ``_tracking_wrapper`` tracks on return and recovers in its ``except``, and
#: neither runs if the process is killed. See :func:`recover_in_flight`.
_in_flight: List[Tuple[Any, Any, Tuple[Any, ...], Dict[str, Any]]] = []

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

    An unrecognisable object counts as real, including one whose ``_manager``
    is ``None``: a fake deployment swept is a round trip and a warning, whereas
    a real one skipped is a cluster left running and billing. That bias lives
    in :func:`_creator_is_mocked`, which this defers to for everything but the
    receiver itself.
    """
    from unittest.mock import NonCallableMock

    if isinstance(obj, NonCallableMock):
        return True
    manager = getattr(obj, '_manager', None)
    if isinstance(manager, NonCallableMock):
        return True
    return _creator_is_mocked(manager)


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


def _recover_orphan(
    receiver: Any,
    finder: Any,
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
) -> None:
    """
    Track the deployment a *failed* creation call left running.

    Every creator brings the deployment into existence and only then waits for
    it: ``create_cluster`` has its ``get_cluster`` before ``_wait_on_state``
    (``management/v2/cluster.py:1426``). So a wait that times out, hits a
    transient error, or is interrupted raises *after* the server has a live,
    billable deployment -- and since tracking wraps the return value, nothing
    is ever registered. That leak is silent: no per-class sweep, no
    end-of-session sweep, and no mention in the summary.

    The name is the first argument to every creator, so the orphan can be
    found by listing and matching on it. Failures here are logged, not raised:
    this runs while another exception is propagating, and replacing the
    caller's error with a cleanup error would hide the real failure.
    """
    name = kwargs.get('name') or (args[0] if args else None)
    if not isinstance(name, str):
        return

    try:
        for obj in finder(receiver):
            if getattr(obj, 'name', None) != name:
                continue
            track(
                obj,
                '{} {!r} (left behind by a failed create)'.format(
                    type(obj).__name__, name,
                ),
            )
            return
    except Exception as exc:
        logger.warning(
            f'Could not look for a deployment named {name!r} left behind by '
            f'a failed create; it may still be running: {exc}',
        )


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


def _creator_is_mocked(target: Any) -> bool:
    """
    Is this creation call going through a mocked manager?

    The unit tests call the creation methods with ``_post`` patched, and the
    objects they get back name deployments that do not exist, so they must not
    be tracked. ``target`` is the creation call's receiver -- a manager, or the
    ``WorkspaceGroup`` of ``WorkspaceGroup.create_workspace`` -- or, through
    :func:`_is_mocked`, whatever a created object holds in ``_manager``.
    """
    from unittest.mock import NonCallableMock

    if isinstance(target, NonCallableMock):
        return True
    manager = target if hasattr(target, '_post') else getattr(
        target, '_manager', None,
    )
    if isinstance(manager, NonCallableMock):
        return True
    # An unrecognisable receiver counts as real: a fake deployment swept is a
    # round trip and a warning, whereas a real one skipped is a cluster left
    # running and billing.
    return any(
        isinstance(getattr(manager, x, None), NonCallableMock)
        for x in ('_get', '_post', '_delete')
    )


#: (module, class, method, finder) tuples for the calls that bring a billable
#: deployment into existence. Wrapping them is what makes tracking automatic,
#: so a new test cannot leak a cluster by forgetting to register it.
#:
#: ``finder`` takes the receiver -- the manager, or the group for
#: ``WorkspaceGroup.create_workspace`` -- and returns the collection to search
#: for a deployment the call created but did not return. See
#: :func:`_recover_orphan`.
_CREATORS = [
    (
        'singlestoredb.management.v1.workspace', 'WorkspaceManager',
        'create_workspace_group',
        lambda recv: recv.workspace_groups,
    ),
    (
        'singlestoredb.management.v1.workspace', 'WorkspaceManager',
        'create_workspace',
        # WorkspaceManager has no `workspaces` of its own, so the search goes
        # group by group. Only ever walked on the failure path.
        lambda recv: [w for g in recv.workspace_groups for w in g.workspaces],
    ),
    (
        'singlestoredb.management.v1.workspace', 'WorkspaceManager',
        'create_starter_workspace',
        lambda recv: recv.starter_workspaces,
    ),
    (
        'singlestoredb.management.v1.workspace', 'WorkspaceGroup',
        'create_workspace',
        lambda recv: recv.workspaces,
    ),
    (
        'singlestoredb.management.v2.cluster', 'ClusterManager',
        'create_cluster',
        lambda recv: recv.clusters,
    ),
    (
        'singlestoredb.management.v2.cluster', 'ClusterManager',
        'create_starter_cluster',
        lambda recv: recv.starter_clusters,
    ),
]

_tracking_installed = False


def _tracking_wrapper(func: Any, finder: Any) -> Any:
    """
    Wrap a creation method so its result -- or its orphan -- gets tracked.

    On success the returned deployment is registered. On failure the
    deployment the call already brought into existence is looked up and
    registered instead; see :func:`_recover_orphan` for why one exists.

    The call is also listed in ``_in_flight`` for its duration, so a sweep
    that runs while it is still waiting -- SIGTERM, atexit -- can recover the
    orphan itself rather than being killed before the ``except`` below gets to
    (see :func:`recover_in_flight`).

    ``_creator_is_mocked``, not ``_is_mocked``: the receiver is the manager (or
    the workspace group), and ``_is_mocked`` looks for a ``_manager``
    attribute, which a manager does not have -- so a real manager with a
    patched ``_post`` would read as live and the recovery would fire a real
    API call from a unit test. ``_creator_is_mocked`` inspects the receiver's
    own transport and handles both receiver shapes.
    """
    import functools

    @functools.wraps(func)
    def wrapper(receiver: Any, *args: Any, **kwargs: Any) -> Any:
        mocked = _creator_is_mocked(receiver)
        entry = (receiver, finder, args, kwargs)
        if not mocked:
            _in_flight.append(entry)
        try:
            return track(func(receiver, *args, **kwargs))
        except BaseException:
            # BaseException, not Exception: a KeyboardInterrupt during the
            # twenty-minute wait_on_active wait leaves the same live
            # deployment behind as a timeout does.
            #
            # Only if the entry is still listed: claiming it is what keeps this
            # from tracking the orphan a second time when a sweep already
            # recovered it mid-wait and then let the call unwind.
            if not mocked and _drop_in_flight(entry):
                _recover_orphan(receiver, finder, args, kwargs)
            raise
        finally:
            if not mocked:
                _drop_in_flight(entry)

    return wrapper


def _drop_in_flight(entry: Tuple[Any, Any, Tuple[Any, ...], Any]) -> bool:
    """
    Remove one in-flight entry, and say whether it was still there.

    By identity, and only this entry: two creations with equal arguments --
    a retried create, say -- would otherwise pop each other's.
    """
    for i, other in reversed(list(enumerate(_in_flight))):
        if other is entry:
            _in_flight.pop(i)
            return True
    return False


def recover_in_flight() -> None:
    """
    Track the deployments that creation calls still in progress have created.

    A creator POSTs, then waits. Everything that registers a deployment runs
    after that wait -- ``track()`` on return, ``_recover_orphan()`` in the
    wrapper's ``except`` -- so a sweep triggered from outside the call, by
    SIGTERM or atexit, sees nothing in ``_tracked`` and the deployment is left
    running. A cancelled CI job during a shared-pool build is exactly that
    case.

    So each in-flight call is looked up the same way a failed one is, putting
    whatever the server already created into ``_tracked`` before the sweep
    walks it. Entries are popped as they are drained, so a handler that
    returns and lets the wrapper's own ``except`` run cannot recover twice.
    Never raises: ``_recover_orphan`` logs its own failures, and this runs on
    the way out.
    """
    while _in_flight:
        receiver, finder, args, kwargs = _in_flight.pop()
        _recover_orphan(receiver, finder, args, kwargs)


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

    import importlib

    for module_name, class_name, method_name, finder in _CREATORS:
        try:
            klass = getattr(importlib.import_module(module_name), class_name)
            setattr(
                klass, method_name,
                _tracking_wrapper(getattr(klass, method_name), finder),
            )
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
    server.

    Only a 404 counts as gone. Any other refresh failure reports "still
    there": answering "gone" on a transient 5xx or a dropped connection
    skips the termination below, and a cluster left running costs money,
    whereas a redundant terminate on something already gone is one wasted
    round trip.
    """
    if hasattr(obj, 'refresh'):
        try:
            obj.refresh()
        except ManagementError as exc:
            if exc.errno == 404:
                return True
            logger.warning(
                f'Could not refresh {obj!r} to see whether it is already '
                f'gone; assuming it is still live: {exc}',
            )
            return False
        except Exception as exc:
            logger.warning(
                f'Could not refresh {obj!r} to see whether it is already '
                f'gone; assuming it is still live: {exc}',
            )
            return False
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

    removed = []
    for entry in entries:
        _, label, obj = entry
        if _is_gone(obj):
            _tracked.remove(entry)
            continue
        try:
            terminate(obj)
        except Exception as exc:
            # Deliberately left in ``_tracked``, so the end-of-session sweep
            # tries again. Dropping the entry first -- as this used to -- meant
            # one transient error was enough to leak the deployment for good,
            # and it did not even appear in the summary below.
            logger.warning(f'Could not terminate {label}: {exc}')
        else:
            _tracked.remove(entry)
            removed.append(label)
    return removed


def tracked_labels() -> List[str]:
    """
    Labels of every deployment still tracked, i.e. not yet swept.

    After the end-of-session sweep this should be empty; anything left is a
    deployment that is still live and still costing money, so conftest
    reports it rather than letting the run end quietly.
    """
    return [label for _, label, _ in _tracked]


#
# Shared deployment pool
#
# Several classes need nothing from a deployment but that it is live: the
# Stage and Job suites read and write through the management API against
# whatever cluster they are handed. Deploying one apiece cost 2190s of the
# 8915s a traced run took, and an S-00 cluster reaching ACTIVE is ~460s that
# cannot be made faster -- so the only lever is deploying fewer of them.
#
# The pool is built on first use and reused for the rest of the process. A
# class must not mutate what it borrows: anything that PATCHes, suspends or
# terminates its subject keeps deploying its own (see
# ``docs/shared-deployment-pool-plan.md`` for which classes those are and why).
#
# The pool is process-wide, so under ``pytest-xdist`` every worker that gets a
# borrowing class builds a pool of its own. The ``xdist_group`` marks below
# keep the borrowers together on a worker; see ``SHARED_CLUSTER_*_GROUP``.
#

#: ``xdist_group`` names for the classes that borrow from the pool, so
#: ``--dist loadgroup`` puts each set on one worker and each set builds one
#: pool. Two groups rather than one: a single group serialises all four classes
#: behind one pool build, and the groups run concurrently on separate workers,
#: so splitting costs one extra cluster and halves that chain.
#:
#: Stage wants two clusters (``TestStageFusion`` names a second one in
#: ``IN GROUP``) and jobs want one, so the split follows what they borrow:
#:
#: * ``SHARED_CLUSTER_STAGE_GROUP`` -- ``TestStageFusion``, v2 ``TestStage``
#: * ``SHARED_CLUSTER_JOBS_GROUP`` -- ``TestJobsFusion``, v2 ``TestJob``
#:
#: Without ``-n``/``--dist loadgroup`` the marks do nothing: one process, one
#: pool of two, which is the serial behaviour they were added on top of.
SHARED_CLUSTER_STAGE_GROUP = 'shared-cluster-stage'
SHARED_CLUSTER_JOBS_GROUP = 'shared-cluster-jobs'

#: Live clusters shared by the classes that need only *a* deployment.
_pool: List[Any] = []

#: Why the pool cannot be built in this organization, once that is known.
#: Cached so the second class to ask skips without repeating the lookups.
_pool_skip: Optional[str] = None

#: Suffix for the pool's cluster names, so a run's clusters are distinguishable
#: from a concurrent run's. Matches the ``cl-test-*`` pattern the maintenance
#: sweep in ``cleanup_deployments.py`` looks for.
_pool_id = secrets.token_hex(4)


def shared_clusters(count: int = 1) -> List[Any]:
    """
    Return ``count`` live v2 clusters shared by the whole test session.

    The pool grows to fit the largest request and is never rebuilt, so every
    caller gets the same objects::

        @classmethod
        def setUpClass(cls):
            cls.cluster, cls.cluster_2 = utils.shared_clusters(2)

    Raises ``unittest.SkipTest`` for the same reasons the per-class fixtures
    did -- no US regions, or no project to deploy into -- so a class that
    borrows from the pool skips where it used to skip.

    Terminating a pool cluster is not this module's business beyond the
    end-of-session sweep: a class that borrows one must leave it live and
    usable, since the classes after it get the same object.
    """
    global _pool_skip

    if _pool_skip:
        raise unittest.SkipTest(_pool_skip)

    if len(_pool) >= count:
        return _pool[:count]

    # Pinned to v2: the pool's consumers are v2 suites, so the fixture must
    # not follow the management.version option out of v2 either.
    mgr = s2.manage_clusters(version='v2')

    us_regions = [
        x for x in mgr.regions
        if 'US' in x.name or 'us-' in (x.region_name or '')
    ]
    if not us_regions:
        _pool_skip = 'No US regions reported by the v2 API'
        raise unittest.SkipTest(_pool_skip)

    project_id = os.environ.get('SINGLESTOREDB_TEST_PROJECT')
    if not project_id:
        standard = [x for x in mgr.projects if x.edition == 'STANDARD']
        if not standard:
            _pool_skip = (
                'No STANDARD project in this organization; set '
                'SINGLESTOREDB_TEST_PROJECT to the project to deploy into'
            )
            raise unittest.SkipTest(_pool_skip)
        project_id = standard[0].id

    # Tracked under the empty owner rather than under whichever class happened
    # to ask first. conftest.pytest_runtest_setup sweeps the previous owner's
    # deployments as soon as the run moves to the next class, so a pool
    # attributed to a class would be terminated after its first consumer;
    # ``''`` matches no per-class sweep and is swept exactly once, by
    # pytest_unconfigure, which passes owner=None and so matches everything.
    prev = get_owner()
    set_owner('')
    try:
        while len(_pool) < count:
            _pool.append(
                mgr.create_cluster(
                    f'cl-test-shared-{len(_pool)}-{_pool_id}',
                    region=random.choice(us_regions),
                    size='S-00',
                    # The v2 suites that deploy their own ask for this, and a
                    # pool cluster stands in for those, so it has to be at
                    # least as reachable as what it replaces.
                    firewall_ranges=['0.0.0.0/0'],
                    project=project_id,
                    wait_on_active=True,
                    wait_timeout=1200,
                ),
            )
    finally:
        set_owner(prev)

    return _pool[:count]


class CountingManager:
    """
    Stand-in for a :class:`Manager` that records every request.

    Enough of the management API's filesystem behaviour is simulated for a
    :class:`Stage` or :class:`FileSpace` to be driven end to end without a
    deployment: paths listed in ``existing`` answer metadata requests, and
    anything else raises the 404 ``ManagementError`` that ``exists`` reads.
    Writes and deletes update that set, so a sequence of operations sees the
    effect of the ones before it.

    ``calls`` holds one ``(method, path)`` pair per request, in order, which
    is what makes the round-trip count of an operation assertable. The paths
    are the remote path the caller asked for, with the route prefix
    (``clusters/<id>/stage/fs/``, ``files/fs/<space>/``) and any query string
    removed, so the same expectations read the same for Stage and for a file
    space.

    Parameters
    ----------
    existing : iterable of str, optional
        Remote paths that already exist. A path ending in ``/`` is a folder.

    """

    def __init__(self, existing: Any = ()):
        self.existing = {self._key(x) for x in existing}
        self.calls: List[Tuple[str, str]] = []

    @staticmethod
    def _key(path: Any) -> str:
        """Reduce a request path to the remote path it addresses."""
        path = str(path).split('?')[0]
        # 'files/fs/<space>/<path>' for a file space, '<resource>/fs/<path>'
        # for a Stage at either version
        path = re.sub(r'^files/fs/[^/]+/', r'', path)
        path = re.split(r'/fs/', path, maxsplit=1)[-1]
        # A trailing '/' marks a folder, but the routes collapse runs of them
        return re.sub(r'/+$', r'/', path).lstrip('/')

    def _response(self, key: str) -> Any:
        """Return a metadata response for an existing path."""
        is_dir = key.endswith('/')
        return SimpleNamespace(
            json=lambda: dict(
                name=key.rstrip('/').rsplit('/', 1)[-1],
                path=key,
                size=0 if is_dir else 8,
                type='directory' if is_dir else 'file',
                format='',
                mimetype='' if is_dir else 'text/plain',
                writable=True,
                content=[] if is_dir else None,
            ),
            content=b'' if is_dir else b'contents',
        )

    def _get(self, path: Any, params: Any = None, **kwargs: Any) -> Any:
        key = self._key(path)
        self.calls.append(('GET', key))
        if key not in self.existing:
            # A folder resolves whether or not the caller asked for it with a
            # trailing '/', the way the routes behave
            if not key.endswith('/') and f'{key}/' in self.existing:
                return self._response(f'{key}/')
            raise ManagementError(errno=404, msg=f'path does not exist: {key}')
        return self._response(key)

    def _put(self, path: Any, **kwargs: Any) -> Any:
        key = self._key(path)
        if 'isFile=false' in str(path):
            key = re.sub(r'/*$', r'/', key)
        self.calls.append(('PUT', key))
        self.existing.add(key)
        return SimpleNamespace(
            json=lambda: dict(name=key.rsplit('/', 1)[-1], path=key),
            content=b'',
        )

    def _patch(self, path: Any, json: Any = None, **kwargs: Any) -> Any:
        key = self._key(path)
        self.calls.append(('PATCH', key))
        self.existing.discard(key)
        self.existing.add(self._key((json or {}).get('newPath', key)))
        return SimpleNamespace(json=lambda: {}, content=b'')

    def _delete(self, path: Any, **kwargs: Any) -> Any:
        key = self._key(path)
        self.calls.append(('DELETE', key))
        self.existing.discard(key)
        return SimpleNamespace(json=lambda: {}, content=b'')

    def counts(self) -> Dict[str, int]:
        """Return the number of recorded requests per method."""
        out: Dict[str, int] = {}
        for method, _ in self.calls:
            out[method] = out.get(method, 0) + 1
        return out


def counting_stage(existing: Any = (), stage_cls: Any = None) -> Tuple[Any, Any]:
    """
    Return a ``(Stage, CountingManager)`` pair wired to no deployment.

    Parameters
    ----------
    existing : iterable of str, optional
        Stage paths that already exist
    stage_cls : type, optional
        ``Stage`` class to instantiate. Defaults to the version-neutral one;
        pass ``v1.stage.Stage`` to exercise the v1 route prefix, which the
        recorded paths have stripped either way.

    """
    if stage_cls is None:
        from singlestoredb.management.stage import Stage as stage_cls
    manager = CountingManager(existing)
    stage = stage_cls.__new__(stage_cls)
    stage._deployment_id = 'deployment-id'
    stage._manager = manager
    return stage, manager


def counting_file_space(existing: Any = ()) -> Tuple[Any, Any]:
    """
    Return a ``(FileSpace, CountingManager)`` pair wired to no organization.

    Parameters
    ----------
    existing : iterable of str, optional
        File paths that already exist

    """
    from singlestoredb.management.files import FileSpace
    manager = CountingManager(existing)
    space = FileSpace.__new__(FileSpace)
    space._location = 'personal'
    space._manager = manager
    return space, manager


#: IDs for the fixtures :func:`counting_cluster_manager` builds by default.
COUNTING_CLUSTER_NAME = 'counting-cluster'
COUNTING_CLUSTER_ID = 'ffffffff-0000-0000-0000-000000000001'
COUNTING_PROJECT_ID = 'ffffffff-0000-0000-0000-000000000002'


def cluster_payload(
    name: str,
    id: str,
    project_id: Optional[str] = None,
    region: Optional[str] = None,
    **extra: Any,
) -> Dict[str, Any]:
    """
    Return one item of a ``GET /v2/clusters`` response.

    ``region`` is omitted unless asked for: a payload that carries one makes
    ``Cluster.from_dict`` resolve it against ``ClusterManager.regions``, which
    is a request of its own, and the callers here are counting the requests an
    upload makes rather than that one.

    Parameters
    ----------
    name : str
        Name of the cluster
    id : str
        Cluster ID
    project_id : str, optional
        Value for ``projectID``
    region : str, optional
        Value for ``region``, the provider region name
    **extra : keyword arguments, optional
        Further response keys, in the API's own spelling

    """
    out: Dict[str, Any] = dict(
        name=name, clusterID=id, state='ACTIVE',
        sizeConfig=dict(size='S-00', scaleFactor=1.0),
    )
    if project_id is not None:
        out['projectID'] = project_id
    if region is not None:
        out['region'] = region
    out.update(extra)
    return out


def project_payload(
    id: str,
    name: str,
    edition: str = 'STANDARD',
) -> Dict[str, Any]:
    """Return one item of a ``GET /v2/projects`` response."""
    return dict(projectID=id, name=name, edition=edition)


class CountingClusterManager(_ClusterManager):
    """
    A :class:`ClusterManager` that answers from fixtures and records requests.

    This is :class:`CountingManager` widened to a whole Fusion statement: the
    management routes a statement resolves its deployment through
    (``clusters``, ``clusters/<id>``, ``projects``, ``regions``,
    ``sharedtier/virtualClusters``) are served from the lists given here, and
    Stage's own filesystem routes are delegated to a :class:`CountingManager`
    sharing this object's ``calls`` list, so one ordered record covers both.

    Any other route raises, so a request nobody accounted for cannot slip
    through as a mock's default return value.

    Parameters
    ----------
    clusters : list of dict, optional
        ``GET /v2/clusters`` items; see :func:`cluster_payload`
    projects : list of dict, optional
        ``GET /v2/projects`` items; see :func:`project_payload`
    starter_clusters : list of dict, optional
        ``GET /v2/sharedtier/virtualClusters`` items
    regions : list of dict, optional
        ``GET /v2/regions`` items
    existing : iterable of str, optional
        Stage paths that already exist; a path ending in ``/`` is a folder

    """

    def __init__(
        self,
        clusters: Any = None,
        projects: Any = None,
        starter_clusters: Any = (),
        regions: Any = (),
        existing: Any = (),
    ):
        # Deliberately not calling ClusterManager.__init__: it wants an access
        # token and a base URL, and nothing here makes a request.
        if clusters is None:
            clusters = [
                cluster_payload(
                    COUNTING_CLUSTER_NAME, COUNTING_CLUSTER_ID,
                    project_id=COUNTING_PROJECT_ID,
                ),
            ]
        if projects is None:
            projects = [project_payload(COUNTING_PROJECT_ID, 'Test Project')]

        self._cluster_payloads = list(clusters)
        self._project_payloads = list(projects)
        self._starter_cluster_payloads = list(starter_clusters)
        self._region_payloads = list(regions)

        #: Serves the Stage filesystem routes
        self.files = CountingManager(existing)

        #: One ``(method, path)`` pair per request, in order
        self.calls = self.files.calls

    def _get(self, path: Any, params: Any = None, **kwargs: Any) -> Any:
        if '/fs/' in str(path):
            return self.files._get(path, params=params, **kwargs)

        key = str(path).split('?')[0]
        self.calls.append(('GET', key))

        if key == 'clusters':
            return SimpleNamespace(json=lambda: self._cluster_payloads)
        if key == 'projects':
            return SimpleNamespace(json=lambda: self._project_payloads)
        if key == 'regions':
            return SimpleNamespace(json=lambda: self._region_payloads)
        if key == 'sharedtier/virtualClusters':
            return SimpleNamespace(json=lambda: self._starter_cluster_payloads)

        if key.startswith('clusters/'):
            wanted = key.split('/', 1)[1]
            for item in self._cluster_payloads:
                if item['clusterID'] == wanted:
                    return SimpleNamespace(json=lambda item=item: item)
            raise ManagementError(errno=404, msg=f'cluster not found: {wanted}')

        raise AssertionError(f'unexpected request: GET {key}')

    def _put(self, path: Any, **kwargs: Any) -> Any:
        if '/fs/' in str(path):
            return self.files._put(path, **kwargs)
        raise AssertionError(f'unexpected request: PUT {path}')

    def _patch(self, path: Any, **kwargs: Any) -> Any:
        if '/fs/' in str(path):
            return self.files._patch(path, **kwargs)
        raise AssertionError(f'unexpected request: PATCH {path}')

    def _delete(self, path: Any, **kwargs: Any) -> Any:
        if '/fs/' in str(path):
            return self.files._delete(path, **kwargs)
        raise AssertionError(f'unexpected request: DELETE {path}')

    def _post(self, path: Any, **kwargs: Any) -> Any:
        raise AssertionError(f'unexpected request: POST {path}')

    def counts(self) -> Dict[str, int]:
        """Return the number of recorded requests per method."""
        return self.files.counts()


def counting_cluster_manager(**kwargs: Any) -> CountingClusterManager:
    """Return a :class:`CountingClusterManager`; see it for the arguments."""
    return CountingClusterManager(**kwargs)


def run_fusion_statement(sql: str, manager: Any) -> Any:
    """
    Execute one Fusion statement against a counting cluster manager.

    The statement is parsed and run the way a cursor would run it, so the
    requests recorded on ``manager.calls`` are the ones the whole statement
    costs -- deployment resolution included -- rather than the ones a single
    :class:`Stage` call makes.

    Parameters
    ----------
    sql : str
        The Fusion statement
    manager : CountingClusterManager
        The manager every handler in the statement resolves through

    Returns
    -------
    FusionSQLResult

    """
    from singlestoredb.fusion import registry
    from singlestoredb.fusion.handlers import cluster as cluster_handlers
    from singlestoredb.fusion.handlers import utils as handler_utils

    # The results are formatted against the connection's decoders; there is no
    # connection here and nothing to decode.
    conn = SimpleNamespace(decoders={}, _results_type='tuples')

    with mock.patch.dict(os.environ, {'SINGLESTOREDB_FUSION_ENABLED': '1'}):
        handler = registry.get_handler(sql)
        if handler is None:
            raise ValueError(f'no Fusion handler for statement: {sql}')
        with mock.patch.object(
            handler_utils, 'get_cluster_manager', return_value=manager,
        ), mock.patch.object(
            cluster_handlers, 'get_cluster_manager', return_value=manager,
        ):
            return handler(conn).execute(sql)


def clear_stage(deployment: Any) -> None:
    """
    Empty a deployment's stage.

    A pool cluster carries whatever the class before left in its stage, and
    ``TestStageFusion`` asserts exact listings of the stage root, so it starts
    from a known-empty one rather than from whatever ran first. Failures are
    logged rather than raised: this runs in a fixture, where the interesting
    failure is the test's, not the cleanup's.
    """
    stage = deployment.stage

    # The root listing is enough: a folder goes recursively, so there is no
    # reason to enumerate what is inside it.
    for obj in stage.listdir('/', return_objects=True):
        try:
            if obj.type == 'directory':
                stage.removedirs(obj.path)
            else:
                stage.remove(obj.path)
        except Exception as exc:
            logger.warning(f'Could not clear stage path {obj.path}: {exc}')
