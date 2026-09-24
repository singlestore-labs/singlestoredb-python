#!/usr/bin/env python
# type: ignore
"""Utilities for testing."""
import glob
import json
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
# Everything above is in-process, which is the one thing it cannot fix: the
# sweep, the ledger and `tearDownClass` all die with the interpreter. A job
# killed mid-provision -- GitHub force-terminates a cancelled job's remaining
# steps after a five-minute cancellation timeout -- leaves a PENDING cluster
# that no code here will ever get another chance to delete. `expires_at` is
# the answer to that, and only that: it is a property of the deployment, so
# the control plane honours it whether or not this process is still alive.
#

#: Expiry to request on every deployment a test creates, as the duration
#: string `POST` accepts (`resources/create_test_cluster.py` has passed one
#: nightly since it was written). The backstop under the sweep and the ledger,
#: not a replacement for either: a test still terminates what it created, and
#: nothing waits for an expiry to fire.
#:
#: Two hours, against a `wait_timeout` of 1200s and a longest test (Fusion
#: `CREATE`/`DROP`, which provisions twice in sequence) of about twenty
#: minutes. Enough headroom that an expiry can never land on a deployment a
#: test is still using, which would show up as an unrelated flake and be read
#: as an API fault.
#:
#: Applied to what accepts it, which is the deployments that cost: v2
#: `ClusterManager.create_cluster` and v1
#: `WorkspaceManager.create_workspace_group`. The rest take no `expires_at` and
#: need none:
#:
#: * a v1 workspace -- `expiresAt` is a property of the group, and terminating
#:   the group takes its workspaces with it;
#: * the starter deployments -- `create_starter_cluster` and
#:   `create_starter_workspace` have no such argument, and being shared tier
#:   they are not what a leak costs.
DEPLOYMENT_EXPIRES_AT = '2h'

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


#
# Durable deployment ledger
#
# Everything above this point is in-memory only, and that is the one leak the
# sweeps cannot cover. A cancelled CI job is the proven case: GH Actions run
# 35631802648, job ``test-coverage``, was cancelled 19 minutes into
# ``TestClusterFusion.setUpClass``'s ``create_cluster(wait_on_active=True,
# wait_timeout=1200)``. The log ends at ``##[error]The operation was
# canceled.`` with no pytest summary, no "Terminated deployments left behind
# by tests:" and no ``STILL LIVE`` banner -- the process never got to sweep,
# and GitHub's cancellation grace period is nowhere near long enough for
# pytest to unwind three nested class fixtures, list to recover three
# in-flight creations and issue three DELETEs. After the SIGKILL that follows,
# ``_tracked`` and ``_in_flight`` are gone with the process and *nothing on
# disk* records that three clusters were created.
#
# So every creation is also appended to a JSONL file, flushed and fsync'd per
# line, which ``cleanup_deployments.py --ledger`` reads afterwards from a
# separate process -- an ``if: always()`` CI step that still runs on
# cancellation. The ledger is the record; the in-memory sweeps stay exactly as
# they were and remain the fast path.
#
# Opt-in, via SINGLESTOREDB_TEST_DEPLOYMENT_LOG. With the variable unset
# nothing is written and behaviour is byte-for-byte what it was: a local run
# has a human watching it and does not need a file to reap from.
#

#: Ledger event kinds, in the order a deployment normally produces them:
#:
#: * ``pending`` -- the creator is about to be called. Written *before* the
#:   POST, from the name argument, because the whole point is the window where
#:   the server has a billable deployment and this process has no id for it.
#: * ``live`` -- the creation returned (or an orphan was recovered), so there
#:   is an id.
#: * ``gone`` -- it has been terminated.
#:
#: The reaper folds the file: anything whose last event is not ``gone`` is
#: still live. A ``pending`` with no matching ``live`` is the cancelled-mid-
#: wait case, and it is resolved by name rather than by id.

#: Environment variable naming the ledger file. Read per write rather than
#: cached at import so a test can point it at a tmp_path with
#: ``mock.patch.dict(os.environ, ...)``.
LEDGER_ENV_VAR = 'SINGLESTOREDB_TEST_DEPLOYMENT_LOG'

#: Deployment kind for each created object's class. The ledger records a kind
#: so the reaper knows which manager and which point lookup to resolve a
#: record against, instead of guessing from the name -- ``cl-test-abc`` and
#: ``ws-test-abc`` are only distinguishable by convention, and a ``--ledger``
#: run deliberately does not consult :data:`cleanup_deployments.PATTERNS`.
#:
#: Keyed by class name rather than by the class itself to avoid importing v1
#: and v2 management just to write a log line.
_KIND_BY_CLASS = {
    'WorkspaceGroup': 'workspace_group',
    'Workspace': 'workspace',
    'StarterWorkspace': 'starter_workspace',
    'Cluster': 'cluster',
    'StarterCluster': 'starter_cluster',
}


def ledger_path() -> Optional[str]:
    """Path of the deployment ledger, or None if none was configured."""
    return os.environ.get(LEDGER_ENV_VAR) or None


def _ledger_write(**record: Any) -> None:
    """
    Append one record to the deployment ledger.

    Opened, written and closed per record, with ``flush()`` and ``os.fsync()``
    before the handle goes: surviving SIGKILL is the entire purpose, and a
    line still sitting in a buffer when the process dies records nothing. The
    cost is one open per creation, against a creation that takes minutes.

    ``O_APPEND`` plus one ``write()`` per line is what makes this safe for the
    parallel default (``-n 2``): the xdist workers are separate processes
    sharing the file, and a single write of well under PIPE_BUF cannot
    interleave with another's on Linux. No locking, therefore, and no partial
    lines for the reaper to choke on.

    Never raises. This sits on the creation path of every management test, so
    a full disk or an unwritable path must cost a warning, not a test failure.
    """
    path = ledger_path()
    if not path:
        return
    try:
        # default=str so an unexpected value (a datetime, an enum) degrades to
        # its repr instead of raising and losing the whole record.
        line = json.dumps(record, default=str, sort_keys=True) + '\n'
        with open(path, 'a', encoding='utf-8') as file:
            file.write(line)
            file.flush()
            os.fsync(file.fileno())
    except Exception as exc:
        logger.warning(
            f'Could not append {record!r} to the deployment ledger at '
            f'{path!r}; a deployment this run creates may not be reaped: '
            f'{exc}',
        )


def _ledger_kind(obj: Any) -> Optional[str]:
    """Ledger kind for a created object, or None if it is not a deployment."""
    return _KIND_BY_CLASS.get(type(obj).__name__)


def ledger_pending(kind: str, args: Tuple[Any, ...], kwargs: Any) -> None:
    """
    Record that a deployment of this kind is about to be created.

    The name is taken the same way :func:`_recover_orphan` takes it -- keyword
    first, else the first positional -- because it is the first parameter of
    every creator, which ``test_management_utils.py`` pins. A record with no
    usable name is skipped: there would be nothing for the reaper to resolve.
    """
    name = kwargs.get('name') or (args[0] if args else None)
    if not isinstance(name, str):
        return
    _ledger_write(event='pending', kind=kind, name=name)


def ledger_live(obj: Any) -> None:
    """Record that a created deployment exists, now that it has an id."""
    kind = _ledger_kind(obj)
    if kind is None:
        return
    _ledger_write(
        event='live', kind=kind,
        id=getattr(obj, 'id', None),
        name=getattr(obj, 'name', None),
    )


def ledger_gone(obj: Any) -> None:
    """
    Record that a deployment has been terminated.

    Carries the name as well as the id so it also cancels a ``pending``
    record: an orphan recovered by name and then swept in-process would
    otherwise still be listed as live by the reaper.
    """
    kind = _ledger_kind(obj)
    if kind is None:
        return
    _ledger_write(
        event='gone', kind=kind,
        id=getattr(obj, 'id', None),
        name=getattr(obj, 'name', None),
    )


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
        # Here rather than in the wrapper, so an orphan that `_recover_orphan`
        # digs out of a listing gets an id into the ledger too -- that path
        # reaches the server only through this function.
        ledger_live(obj)
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
    found = False
    for i, entry in reversed(list(enumerate(_tracked))):
        if entry[2] is obj:
            _tracked.pop(i)
            found = True
    # Only for something that was actually tracked: untracking an object that
    # was never registered -- a mocked one, or one already swept -- says
    # nothing about whether a real deployment is gone, and a spurious ``gone``
    # would hide a live cluster from the reaper.
    if found:
        ledger_gone(obj)


#: How long :func:`terminate` keeps retrying a deployment the API will not
#: delete yet, and how long it waits between attempts. Three minutes at 15s
#: spacing: the case being covered is a deployment killed mid-provision, which
#: has to finish coming up before it can be torn down, and an S-00 cluster
#: reaching ACTIVE is ~460s at worst. Waiting the full provision out here would
#: stall the sweep between every test class, so this buys the common case --
#: a deployment most of the way up -- and leaves the rest to the end-of-session
#: sweep and then to ``cleanup_deployments.py``, which is the end of the line
#: and waits out a full provision with a longer budget of its own
#: (``cleanup_deployments.TERMINATE_TIMEOUT``).
TERMINATE_RETRY_TIMEOUT = 180.0
TERMINATE_RETRY_INTERVAL = 15.0


def _terminate_once(obj: Any) -> None:
    """
    Issue one terminate, whatever this kind's signature looks like.

    ``force=True`` is what makes a workspace group with live workspaces in it
    go away; the starter variants (``StarterWorkspace.terminate``,
    ``StarterCluster.terminate``) take no arguments at all.

    The signature is inspected rather than discovered by catching ``TypeError``
    from the call, as this used to do. That ``except TypeError`` also caught a
    ``TypeError`` raised from *inside* a terminate that did accept ``force``,
    and then retried without it -- two DELETEs for one deployment, the second
    of them not forced, which is the one shape that leaves a workspace group
    behind.
    """
    import inspect

    try:
        params = inspect.signature(obj.terminate).parameters
    except (TypeError, ValueError):  # pragma: no cover - unintrospectable
        # A builtin or a C-level callable. Fall back to the old behaviour.
        params = {}

    if 'force' in params:
        obj.terminate(force=True)
    else:
        obj.terminate()


def terminate(
    obj: Any,
    timeout: float = TERMINATE_RETRY_TIMEOUT,
    interval: float = TERMINATE_RETRY_INTERVAL,
) -> None:
    """
    Terminate a deployment, whatever kind it is, retrying a 4xx refusal.

    A deployment killed mid-provision is ``PENDING``/``TRANSITIONING``, and the
    API refuses to delete it in that state with a 400 or a 409. Nothing retries
    that: ``Manager.RETRY_STATUSES`` is ``{429, 500, 502, 503, 504}``
    (``management/manager.py:49``), urllib3 only retries what is in that list,
    and there is no wait-until-deletable helper anywhere in the SDK. So the
    per-class sweep logged a warning, the session-end sweep tried exactly once
    more -- usually still too early -- and the deployment was left running.

    Hence the bounded retry here. Only 4xx other than 404 is retried:

    * 404 means it is already gone, so retrying would burn the whole budget
      waiting for something that will never come back. Re-raised, as before,
      which is also what ``_is_gone()`` upstream normally prevents.
    * 5xx and 429 are already retried inside the transport, so seeing one here
      means the transport gave up; another round trip from this layer is not
      what fixes it.

    Raises the last error if the budget runs out, so ``cleanup_tracked()``
    keeps the deployment tracked and the session-end sweep gets another go.
    """
    import time

    deadline = time.monotonic() + timeout
    while True:
        try:
            _terminate_once(obj)
            return
        except ManagementError as exc:
            errno = exc.errno
            if errno is None or errno == 404 or not 400 <= errno < 500:
                raise
            # No budget left for another attempt *plus* the wait before it.
            if time.monotonic() + interval >= deadline:
                raise
            logger.info(
                f'{obj!r} is not deletable yet ({exc}); retrying the '
                f'terminate in {interval:g}s',
            )
            time.sleep(interval)


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


#: (module, class, method, kind, finder) tuples for the calls that bring a
#: billable deployment into existence. Wrapping them is what makes tracking
#: automatic, so a new test cannot leak a cluster by forgetting to register it.
#:
#: ``kind`` is the ledger kind the call produces, and must be a value of
#: :data:`_KIND_BY_CLASS`: it is what lets the ``pending`` record -- written
#: before the POST, when nothing has an id yet -- say which manager the reaper
#: should search. It is stated here rather than derived from ``method_name``
#: because ``create_workspace`` appears twice, on two different receivers.
#:
#: ``finder`` takes the receiver -- the manager, or the group for
#: ``WorkspaceGroup.create_workspace`` -- and returns the collection to search
#: for a deployment the call created but did not return. See
#: :func:`_recover_orphan`.
_CREATORS = [
    (
        'singlestoredb.management.v1.workspace', 'WorkspaceManager',
        'create_workspace_group', 'workspace_group',
        lambda recv: recv.workspace_groups,
    ),
    (
        'singlestoredb.management.v1.workspace', 'WorkspaceManager',
        'create_workspace', 'workspace',
        # WorkspaceManager has no `workspaces` of its own, so the search goes
        # group by group. Only ever walked on the failure path.
        lambda recv: [w for g in recv.workspace_groups for w in g.workspaces],
    ),
    (
        'singlestoredb.management.v1.workspace', 'WorkspaceManager',
        'create_starter_workspace', 'starter_workspace',
        lambda recv: recv.starter_workspaces,
    ),
    (
        'singlestoredb.management.v1.workspace', 'WorkspaceGroup',
        'create_workspace', 'workspace',
        lambda recv: recv.workspaces,
    ),
    (
        'singlestoredb.management.v2.cluster', 'ClusterManager',
        'create_cluster', 'cluster',
        lambda recv: recv.clusters,
    ),
    (
        'singlestoredb.management.v2.cluster', 'ClusterManager',
        'create_starter_cluster', 'starter_cluster',
        lambda recv: recv.starter_clusters,
    ),
]

_tracking_installed = False


def _tracking_wrapper(func: Any, kind: str, finder: Any) -> Any:
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

    That same verdict also decides whether the *result* is tracked, rather than
    leaving it to ``track()``. ``track()`` can only judge what it is handed,
    and it is deliberately biased toward "real" for anything it cannot place --
    including an object whose ``_manager`` is ``None``, which is exactly what a
    unit test's stubbed ``get_cluster`` returns. Nothing a mocked creator
    returns names a deployment that exists, so the receiver's verdict is the
    authoritative one and it is the one used here.

    The ``pending`` ledger record is written here, and deliberately *before*
    ``func`` is called rather than after: from the moment the creator POSTs
    there is a billable deployment, and everything that could record it --
    ``track()`` on return, ``_recover_orphan()`` in the ``except``,
    ``recover_in_flight()`` from a signal handler -- runs after the wait that
    a cancelled CI job never survives. A ``pending`` line on disk is the only
    thing that outlives a SIGKILL there.
    """
    import functools

    @functools.wraps(func)
    def wrapper(receiver: Any, *args: Any, **kwargs: Any) -> Any:
        mocked = _creator_is_mocked(receiver)
        entry = (receiver, finder, args, kwargs)
        if not mocked:
            _in_flight.append(entry)
            ledger_pending(kind, args, kwargs)
        try:
            out = func(receiver, *args, **kwargs)
            return out if mocked else track(out)
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

    for module_name, class_name, method_name, kind, finder in _CREATORS:
        try:
            klass = getattr(importlib.import_module(module_name), class_name)
            setattr(
                klass, method_name,
                _tracking_wrapper(getattr(klass, method_name), kind, finder),
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
            # The server says it is gone, which is exactly what the ledger's
            # ``gone`` means -- a test that terminated in its own teardown gets
            # its record closed here rather than leaving the reaper to look up
            # an id that 404s.
            ledger_gone(obj)
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
            ledger_gone(obj)
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
# class must not mutate what it borrows, so anything whose subject *is* the
# deployment keeps deploying its own: ``TestCluster`` and ``TestWorkspace``
# (``test_update`` PATCHes the cluster and cycles it back through PENDING),
# ``TestClusterFusionCreateDrop`` and ``TestClusterFusionSuspendResume``. So
# does ``TestWorkspaceFusion``, whose workspace groups are the subject of its
# ``SHOW WORKSPACE GROUPS`` assertions and cost 40s to deploy unwaited anyway.
#
# What makes the borrowers safe is that each scopes its assertions to itself:
# every Stage path is namespaced with the class's ``cls.id``, job listings
# filter by job id rather than listing a deployment's jobs, and none of them
# asserts a row count over an org-wide listing.
#
# ``TestClusterFusion`` is the one borrower that does count rows, because
# ``SHOW CLUSTERS ... LIKE`` is what it tests. It stays inside that rule by
# counting over :func:`shared_cluster_pattern` -- which matches this process's
# pool and nothing else -- against :func:`shared_cluster_names` rather than a
# literal, so growing the pool cannot break it.
#
# The pool is process-wide, so under ``pytest-xdist`` every worker that gets a
# borrowing class builds a pool of its own. The ``xdist_group`` marks below
# keep the borrowers together on a worker; see ``SHARED_CLUSTER_*_GROUP``.
#

#: ``xdist_group`` names for the classes that borrow from the pool, so
#: ``--dist loadgroup`` puts each set on one worker and each set builds one
#: pool. Two groups rather than one: a single group serialises every borrower
#: behind one pool build, and the groups run concurrently on separate workers,
#: so splitting costs one extra cluster and halves that chain.
#:
#: The split follows what each set borrows -- three for Stage, one for Jobs:
#:
#: * ``SHARED_CLUSTER_STAGE_GROUP`` -- ``TestStageFusion`` (two; it names a
#:   second in ``IN GROUP``), v2 ``TestStage`` (one), ``TestClusterFusion``
#:   (three, for its ``LIKE``/``ORDER BY``/``LIMIT`` rows)
#: * ``SHARED_CLUSTER_JOBS_GROUP`` -- ``TestJobsFusion``, v2 ``TestJob``
#:
#: ``TestClusterFusion`` sits with Stage rather than Jobs deliberately: the
#: pool grows to the largest request, so putting the class that wants three
#: with the group that already wants two costs one extra cluster, where
#: putting it with Jobs would cost two and leave Stage's pool untouched.
#:
#: Without ``-n``/``--dist loadgroup`` the marks do nothing: one process, one
#: pool of three, which is the serial behaviour they were added on top of.
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
                    expires_at=DEPLOYMENT_EXPIRES_AT,
                    project=project_id,
                    wait_on_active=True,
                    wait_timeout=1200,
                ),
            )
    finally:
        set_owner(prev)

    return _pool[:count]


def shared_cluster_pattern() -> str:
    """
    ``LIKE`` pattern matching this process's pool clusters and nothing else.

    The suffix is what scopes it: ``_pool_id`` is minted per process, so a
    concurrent run's pool -- or another xdist worker's -- does not match, and
    neither does any other ``cl-test-*`` deployment.

    For a suite asserting an exact row count over the pool, pair it with
    :func:`shared_cluster_names` rather than a literal: the pool grows to the
    largest request any class makes, so the number is not fixed at import.
    """
    return f'cl-test-shared-%-{_pool_id}'


def shared_cluster_names() -> List[str]:
    """
    Names of every cluster in the pool as it stands right now.

    Read at assertion time, not cached: a class that runs later and asks for
    more clusters than this one did grows the pool, and an expectation built
    from a literal count would go stale the moment that happened.
    """
    return [x.name for x in _pool]


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

    ``region`` is omitted unless asked for, so a caller can say which of the
    lazy properties it is exercising: reading ``Cluster.region`` resolves the
    name against ``ClusterManager.regions``, which is a request, and only a
    payload carrying a region has anything to resolve.

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
