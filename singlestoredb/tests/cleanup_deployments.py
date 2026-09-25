#!/usr/bin/env python
# type: ignore
r"""
Terminate deployments left behind by earlier test runs.

The test suite now sweeps what it creates (see ``utils.track()`` and the
hooks in ``conftest.py``), but a run that was killed -- or one from before
that sweep existed -- leaves live workspace groups, workspaces, clusters and
starter clusters behind, and they are billed until someone removes them.

Only names the test suite generates are considered, and the default is a dry
run::

    python -m singlestoredb.tests.cleanup_deployments
    python -m singlestoredb.tests.cleanup_deployments --yes

If the organization is visibly full of strays and this reports none, the names
are not in ``PATTERNS``. ``--show-unmatched`` lists every live deployment the
tool does not recognize, which is how an unconventionally named one gets found.

This tool is organization-wide, not run-scoped: it matches on names, and a
name says which suite made a deployment but not which run. A concurrent run's
fixtures look exactly like stranded ones. Age is the only thing separating
them, so ``--older-than`` defaults to a span longer than a full suite rather
than to zero -- raise it if your runs can take longer than that, and only
pass ``--older-than 0`` when you know nothing else is running.

The other direction -- clearing out what a recent session made, rather than
what an old one stranded -- is ``--since``, which replaces the age guard with
a calendar cutoff, and ``--any-name``, which drops the name gate. Clearing
every workspace group created yesterday or today::

    python -m singlestoredb.tests.cleanup_deployments \
        --kind workspace-group --any-name --since yesterday --yes

Those two flags together remove both of the guards that keep this tool off
deployments it did not create, so ``--kind`` matters: without it the same
cutoff sweeps every cluster of that age as well, the shared pool included.
Read the dry run before adding ``--yes``.

For deployments the current process created, nothing here is needed: those
are tracked as they are created and swept per test class by ``conftest.py``,
which cannot see -- or touch -- another run's deployments.

The exception, and the reason this is wired into CI, is ``--ledger``. A run
with ``SINGLESTOREDB_TEST_DEPLOYMENT_LOG`` set records every creation to a
JSONL file as it happens (``utils.ledger_pending``/``ledger_live``/
``ledger_gone``), so a run that was killed outright leaves an exact list of
what it made::

    python -m singlestoredb.tests.cleanup_deployments --ledger deployments.jsonl

That mode replaces *both* guards above. The ledger names deployments rather
than guessing at them, so the patterns are unnecessary; and its entries are
minutes old by construction, so the age filter would spare every one of them.
What keeps it off other people's deployments instead is that it touches only
ids and names the ledger records, and that each CI job writes its own ledger.

Finally, ``--secrets`` sweeps a different subject: the org-scoped secrets
``TestSecrets.test_get_secret`` creates. They bill nothing, but they are
permanent, and the test deletes its own only if it is not killed mid-test::

    python -m singlestoredb.tests.cleanup_deployments --secrets --yes

That is a rolling janitor rather than a run-scoped cleanup -- a secret is named
per-run but a name still says nothing about *which* run, so the age guard is
what keeps this off a live one. It cannot reap the run it is called from; what
it removes is what earlier runs stranded.
"""
import argparse
import datetime
import json
import os
import re
import sys
import warnings
from collections.abc import Container
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import singlestoredb as s2


#: The kinds of deployment this tool can sweep, in the order it lists them.
#: ``--kind`` selects from these; the default is all of them.
KINDS = (
    'cluster',
    'starter-cluster',
    'workspace-group',
    'starter-workspace',
)

#: Hours a deployment must have existed before it is treated as stranded.
#: The slowest class creates three clusters with a 1200s wait each and then
#: terminates them the same way, so a full suite is comfortably inside this;
#: anything younger could belong to a run in progress.
DEFAULT_MIN_AGE_HOURS = 6.0

#: How long to keep retrying a deployment the API will not delete yet. Longer
#: than ``utils.TERMINATE_RETRY_TIMEOUT``, which is short so the between-class
#: sweep cannot stall the suite: nothing runs after this tool, the deployment may
#: still be coming up, ``DELETE`` is refused until it is, and an S-00 cluster
#: reaching ACTIVE is ~460s at worst. The only cost is the CI step's wall clock.
#:
#: An upper bound, not a promise: a *cancelled* job's steps are force-terminated
#: after GitHub's 5-minute cancellation timeout, so a cancel early in a provision
#: gets killed here whatever this says.
TERMINATE_TIMEOUT = 600.0

#: Names the suite generates. Anchored, because these run against a real
#: organization: a pattern that matched a name someone chose by hand would
#: terminate a deployment that is not ours.
PATTERNS = [
    # test_management_v1.py / test_management_v2.py fixtures
    re.compile(r'^(wg|ws|cl)-test-[A-Za-z0-9_-]+$'),
    # TestWorkspace.test_update renames its live group to wg-foo-<token> and
    # never renames it back, so it carries that name for the rest of the class.
    # Unmatched, a group stranded after that test was invisible here.
    re.compile(r'^wg-foo-[A-Za-z0-9_-]+$'),
    re.compile(r'^starter-(ws|cl)-test-[A-Za-z0-9_-]+$'),
    # test_fusion.py fixtures
    re.compile(r'^[A-C] Fusion Testing [0-9a-f]+$'),
    re.compile(r'^[a-z]-fusion-cluster-[0-9a-f]+$'),
    re.compile(r'^jobs-fusion-[0-9a-f]+$'),
    re.compile(r'^stage-fusion-\d-[0-9a-f]+$'),
    # test_create_drop_workspace_group's subject. Hex also covers the decimal
    # id(self) the test used to name it with, so groups stranded by older runs
    # are reaped too.
    re.compile(r'^Create WG Test [0-9a-f]+$'),
]

#: Names the suite used to generate. Kept separate so it is obvious what is only
#: here for cleanup, and matched all the same: a stranded deployment bills
#: whichever revision made it, and ``main`` still creates these with nothing to
#: reap them. Retire an entry once no branch produces the name and the
#: organization is clean of it.
LEGACY_PATTERNS = [
    # TestStageFusion's two workspace groups, before it moved to v2 clusters
    # named stage-fusion-<n>-<id> and then to the shared cluster pool
    re.compile(r'^Stage Fusion Testing \d [0-9a-f]+$'),
    # TestFilesFusion's workspace group, which nothing in the class ever
    # read; it creates no deployment at all now
    re.compile(r'^Files Fusion Testing [0-9a-f]+$'),
    # 'Group <hex>'. No revision of this repo generates this, so it is here on
    # the owner's say-so. Eight hex characters minimum, which is what the ones
    # in the organization have: a plain [0-9a-f]+ would also match the bare
    # 'Group 1' a person or the portal produces.
    re.compile(r'^Group [0-9a-f]{8,}$'),
]


#: Secret names the suite generates. A secret is not a deployment -- it bills
#: nothing and lives on its own route -- so these are swept only when
#: ``--secrets`` asks for it, and never alongside the deployment patterns.
SECRET_PATTERNS = [
    # TestSecrets.test_get_secret, v1 and v2
    re.compile(r'^secret_v[12]_test_[0-9a-f]+$'),
]

#: Secret names earlier revisions generated. Both are fixed rather than
#: per-run, which is what let two concurrent runs delete each other's secret;
#: ``main`` still creates them, so they are still reaped.
LEGACY_SECRET_PATTERNS = [
    re.compile(r'^secret_name$'),
    re.compile(r'^secret_v2_test$'),
]

#: Hours a secret must have existed before it is treated as stranded. Far lower
#: than :data:`DEFAULT_MIN_AGE_HOURS`, because the window it guards is far
#: shorter: the test creates a secret and deletes it in the same test body, a
#: second or two apart, so no secret a live run owns is even minutes old. Not
#: zero, because a run killed between the POST and the DELETE looks exactly
#: like one that is still between them.
DEFAULT_SECRET_MIN_AGE_HOURS = 1.0


def is_test_deployment(name: Optional[str]) -> bool:
    """Was this name generated by the test suite, now or in the past?"""
    if not name:
        return False
    return any(x.match(name) for x in PATTERNS + LEGACY_PATTERNS)


def is_test_secret(name: Optional[str]) -> bool:
    """Was this secret name generated by the test suite, now or in the past?"""
    if not name:
        return False
    return any(x.match(name) for x in SECRET_PATTERNS + LEGACY_SECRET_PATTERNS)


def _created_at(obj: Any) -> Optional[datetime.datetime]:
    """When this deployment was created, or None if the API did not say."""
    created = getattr(obj, 'created_at', None)
    if not isinstance(created, datetime.datetime):
        return None
    if created.tzinfo is None:
        # A naive timestamp from the API is UTC. Reading it as local time
        # would overstate the age by the offset, which is the direction that
        # sweeps a deployment a live run still owns.
        created = created.replace(tzinfo=datetime.timezone.utc)
    return created


def _age_hours(obj: Any) -> Optional[float]:
    """Hours since creation, or None if the API did not report it."""
    created = _created_at(obj)
    if created is None:
        return None
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    return (now - created).total_seconds() / 3600.0


def parse_since(text: str) -> datetime.datetime:
    """
    Read a ``--since`` value as the local midnight starting that day.

    ``today``, ``yesterday`` or an ISO date. The cutoff is local midnight
    rather than a UTC one because the caller is thinking in their own
    calendar days -- "created yesterday" means yesterday where they are.
    """
    today = datetime.date.today()
    if text == 'today':
        day = today
    elif text == 'yesterday':
        day = today - datetime.timedelta(days=1)
    else:
        try:
            day = datetime.date.fromisoformat(text)
        except ValueError:
            raise argparse.ArgumentTypeError(
                f'{text!r} is not a date; expected YYYY-MM-DD, '
                "'today' or 'yesterday'",
            )
    # A naive datetime's astimezone() reads it as local time, which is what
    # gives midnight the caller's offset rather than UTC's.
    return datetime.datetime.combine(day, datetime.time.min).astimezone()


def find_leftovers(
    older_than: float = DEFAULT_MIN_AGE_HOURS,
    include_unknown_age: bool = False,
    since: Optional[datetime.datetime] = None,
    any_name: bool = False,
    kinds: Container[str] = KINDS,
) -> Tuple[List[Tuple[str, Any]], List[str], List[str]]:
    """
    List the live, test-named deployments in the current organization.

    Both API versions are asked: v1 owns workspace groups and workspaces,
    v2 owns clusters, and a suite that has run under either may have left
    something behind.

    ``since`` replaces the ``older_than`` guard with the opposite test --
    created at or after that moment, rather than old enough to be stranded --
    and ``any_name`` drops the name gate, which makes every live deployment a
    candidate. Between them they turn this from "sweep what the suite
    stranded" into "clear out this organization", so ``kinds`` is what keeps
    such a run off deployments the caller did not mean.

    Returns
    -------
    (List[Tuple[str, Any]], List[str], List[str])
        The deployments to sweep, labels for the ones held back by the age
        guard so the caller can say what it did not touch, and labels for the
        live deployments whose names :data:`PATTERNS` does not recognize.

        That third list is the answer to "the organization is full of strays
        and this tool says there are none". A test that names a deployment
        outside the conventions above is invisible here, so it accumulates
        silently -- which is exactly what ``Create WG Test <id(self)>`` did.
        Reporting the unrecognized names makes the next one findable.

    """
    found: List[Tuple[str, Any]] = []
    spared: List[str] = []
    unmatched: List[str] = []

    def keep(obj: Any) -> bool:
        name = getattr(obj, 'name', None)
        if getattr(obj, 'terminated_at', None) is not None:
            return False
        if not any_name and not is_test_deployment(name):
            age = _age_hours(obj)
            unmatched.append(
                '{}{}'.format(
                    name or '<unnamed>',
                    '' if age is None else f' ({age:.1f}h old)',
                ),
            )
            return False

        # Age is the only thing separating a stranded deployment from one a
        # concurrent run is using right now: names carry a per-class random
        # id, not a per-run one, and a cluster name is capped at 32
        # characters, so there is no room to stamp a run id into it.
        created = _created_at(obj)
        if created is None:
            if not include_unknown_age:
                spared.append(f'{name} (creation time not reported)')
                return False
            return True
        if since is not None:
            if created < since:
                spared.append(
                    f'{name} (created {created.astimezone():%Y-%m-%d %H:%M}, '
                    'before the cutoff)',
                )
                return False
            return True
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        age = (now - created).total_seconds() / 3600.0
        if older_than > 0 and age < older_than:
            spared.append(f'{name} ({age:.1f}h old, too new)')
            return False
        return True

    if 'cluster' in kinds or 'starter-cluster' in kinds:
        try:
            clusters = _manager('v2')
        except Exception as exc:
            print(f'! Could not reach management API v2: {exc}', file=sys.stderr)
        else:
            if 'cluster' in kinds:
                for cluster in clusters.clusters:
                    if keep(cluster):
                        found.append((
                            f'cluster {cluster.name} ({cluster.id})', cluster,
                        ))
            if 'starter-cluster' in kinds:
                for starter in clusters.starter_clusters:
                    if keep(starter):
                        found.append((
                            f'starter cluster {starter.name} ({starter.id})',
                            starter,
                        ))

    if 'workspace-group' in kinds or 'starter-workspace' in kinds:
        try:
            workspaces = _manager('v1')
        except Exception as exc:
            print(f'! Could not reach management API v1: {exc}', file=sys.stderr)
        else:
            if 'workspace-group' in kinds:
                for group in workspaces.workspace_groups:
                    if keep(group):
                        # The group takes its workspaces with it, so they are
                        # not listed separately.
                        found.append((
                            f'workspace group {group.name} ({group.id})', group,
                        ))
            if 'starter-workspace' in kinds:
                for starter in workspaces.starter_workspaces:
                    if keep(starter):
                        found.append((
                            f'starter workspace {starter.name} ({starter.id})',
                            starter,
                        ))

    return found, spared, unmatched


#
# Secret mode
#
# Why this is separate from everything above: a secret is org-scoped and
# permanent, it costs nothing to leave lying around, and it is reached through
# ``secrets`` rather than through any deployment listing. It is here because
# ``TestSecrets.test_get_secret`` is the one test that creates an org-scoped
# named object, and nothing in the suite sweeps one as it is created -- a run
# killed between its POST and its DELETE strands a secret for good.
#


def find_stranded_secrets(
    mgr: Any,
    older_than: float = DEFAULT_SECRET_MIN_AGE_HOURS,
    include_unknown_age: bool = False,
) -> Tuple[List[Tuple[str, Any]], List[str], List[str]]:
    """
    List the organization's secrets that the test suite stranded.

    Returns the same three lists as :func:`find_leftovers` -- the secrets to
    delete, labels for the ones the age guard held back, and labels for the
    ones whose names :data:`SECRET_PATTERNS` does not recognize.

    Only one version's manager is needed: ``secrets`` is identical at v1 and
    v2 (see ``management/v2/organization.py``).
    """
    from singlestoredb.management.organization import Secret

    found: List[Tuple[str, Any]] = []
    spared: List[str] = []
    unmatched: List[str] = []

    # Verified live only this far: ``GET secrets`` with no parameters is
    # accepted and answers with a ``secrets`` array -- the ``?name=`` form is
    # all ``Organization.get_secret`` ever sends. UNVERIFIED: that the array is
    # *every* secret in the organization rather than a page of them. It could
    # not be shown against an organization that has none; if the route turns
    # out to paginate, a sweep here is incomplete rather than wrong.
    res = mgr._get('secrets')
    for item in res.json().get('secrets') or []:
        secret = Secret.from_dict(item)

        if secret.deleted_at is not None:
            continue

        age = _age_hours(secret)

        if not is_test_secret(secret.name):
            unmatched.append(
                '{}{}'.format(
                    secret.name or '<unnamed>',
                    '' if age is None else f' ({age:.1f}h old)',
                ),
            )
            continue

        if age is None:
            if not include_unknown_age:
                spared.append(f'{secret.name} (creation time not reported)')
                continue
        elif older_than > 0 and age < older_than:
            spared.append(f'{secret.name} ({age:.1f}h old, too new)')
            continue

        found.append((f'secret {secret.name} ({secret.id})', secret))

    return found, spared, unmatched


def _run_secret_sweep(
    older_than: float,
    include_unknown_age: bool,
    yes: bool,
    show_unmatched: bool,
) -> int:
    """Report, and with ``yes`` delete, the secrets the suite stranded."""
    mgr = _manager('v2')

    try:
        leftovers, spared, unmatched = find_stranded_secrets(
            mgr, older_than, include_unknown_age,
        )
    except Exception as exc:
        # Reported, not raised: this runs as a cleanup step, and a secret bills
        # nothing, so failing the job over one is the wrong trade.
        print(f'! Could not list secrets: {exc}', file=sys.stderr)
        return 1

    if show_unmatched:
        if unmatched:
            print(
                f'{len(unmatched)} secret(s) not recognized as the suite\'s, '
                'and so never swept:',
            )
            for label in sorted(unmatched):
                print(f'  ? {label}')
            print()
        else:
            print('Every secret is recognized by SECRET_PATTERNS.\n')

    if spared:
        print(f'{len(spared)} match(es) left alone by the age filter:')
        for label in spared:
            print(f'  - {label}')
        print()

    if not leftovers:
        print('No stranded test secrets found.')
        return 0

    print(f'{len(leftovers)} stranded test secret(s):')
    for label, _ in leftovers:
        print(f'  - {label}')

    if not yes:
        print('\nDry run; pass --yes to delete these.')
        return 0

    failed = 0
    for label, secret in leftovers:
        try:
            mgr._delete(f'secrets/{secret.id}')
        except Exception as exc:
            failed += 1
            print(f'✗ {label}: {exc}')
        else:
            print(f'✓ deleted {label}')

    return 1 if failed else 0


#
# Ledger mode
#
# Why: GH Actions run 35631802648, job ``test-coverage``, was cancelled 19
# minutes into ``create_cluster(wait_on_active=True)``. The log ends at
# ``##[error]The operation was canceled.`` with no sweep output -- three clusters
# live, no in-process handler ever run. A file written as they are created is the
# only way another process can learn their names.
#

#: How each ledger kind is resolved back to a live object: the management API
#: version that owns it, the point lookup for a record that has an id, and the
#: listing to search by name for a ``pending`` record that never got one.
#:
#: The kinds are the values of ``utils._KIND_BY_CLASS``. An unknown kind is
#: reported rather than skipped, the alternative being to silently not reap it.
LEDGER_KINDS = {
    'cluster': (
        'v2', 'get_cluster', lambda mgr: mgr.clusters,
    ),
    'starter_cluster': (
        'v2', 'get_starter_cluster', lambda mgr: mgr.starter_clusters,
    ),
    'workspace_group': (
        'v1', 'get_workspace_group', lambda mgr: mgr.workspace_groups,
    ),
    'workspace': (
        # WorkspaceManager has no `workspaces` of its own, so the search goes
        # group by group -- the same walk utils._CREATORS uses.
        'v1', 'get_workspace',
        lambda mgr: [w for g in mgr.workspace_groups for w in g.workspaces],
    ),
    'starter_workspace': (
        'v1', 'get_starter_workspace', lambda mgr: mgr.starter_workspaces,
    ),
}


def _manager(version: str) -> Any:
    """Management API manager for ``'v1'`` or ``'v2'``."""
    if version == 'v2':
        return s2.manage_clusters(version='v2')
    # v1 is deprecated, and asking for it here is the point: workspace groups
    # exist nowhere else, so the warning is noise on every run.
    with warnings.catch_warnings():
        warnings.filterwarnings(
            'ignore', category=DeprecationWarning,
            message='.*manage_workspaces.*',
        )
        return s2.manage_workspaces(version='v1')


def fold_ledger(lines: Any) -> List[Dict[str, Any]]:
    """
    Reduce ledger records to the deployments that should still be live.

    The ledger is an append-only history, not a state: a deployment shows up as
    ``pending``, then ``live`` once it has an id, then ``gone`` once terminated.
    Folding keeps every deployment whose last event was not ``gone``.

    A ``pending`` is keyed by ``(kind, name)`` because that is all it has; the
    matching ``live`` retires it and re-keys on the id, so a normal creation's
    two records collapse to one entry. A ``pending`` left standing means the
    creator was interrupted before returning -- the cancelled-mid-wait case,
    resolvable only by name.

    Order is creation order, since dicts preserve insertion order. The caller
    reverses it, so a workspace goes before the group that holds it, matching
    ``utils.cleanup_tracked()``.

    Malformed lines are skipped with a warning rather than aborting: this is the
    last step of a CI job, and one truncated line must not stop the rest from
    being reaped.
    """
    live: Dict[Any, Dict[str, Any]] = {}

    for lineno, line in enumerate(lines, start=1):
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except ValueError as exc:
            print(
                f'! ledger line {lineno} is not JSON, skipping it: {exc}',
                file=sys.stderr,
            )
            continue
        if not isinstance(record, dict):
            continue

        event = record.get('event')
        kind = record.get('kind')
        name = record.get('name')
        ident = record.get('id')

        by_name = ('name', kind, name)
        by_id = ('id', kind, ident)

        if event == 'pending':
            if name is not None:
                live.setdefault(by_name, record)
        elif event == 'live':
            live.pop(by_name, None)
            if ident is not None:
                live[by_id] = record
            elif name is not None:
                # No id in the record: keep it findable by name rather than
                # dropping it. Should not happen, but losing the deployment is
                # the expensive direction.
                live[by_name] = record
        elif event == 'gone':
            if ident is not None:
                live.pop(by_id, None)
            live.pop(by_name, None)

    return list(live.values())


def read_ledger(path: str) -> List[Dict[str, Any]]:
    """
    Fold the ledger at ``path``, newest first.

    A missing file is not an error: the variable can be set on a job whose
    tests created nothing, and a CI cleanup step that failed in that case would
    turn every such run red.
    """
    if not os.path.exists(path):
        print(f'No ledger at {path}; nothing this run created was recorded.')
        return []
    with open(path, encoding='utf-8') as file:
        records = fold_ledger(file)
    # Newest first, so a workspace is terminated before its group.
    records.reverse()
    return records


def find_ledger_leftovers(
    path: str,
) -> Tuple[List[Tuple[str, Any]], List[str], List[str]]:
    """
    Resolve the ledger's still-live records to live deployment objects.

    Returns
    -------
    (List[Tuple[str, Any]], List[str], List[str])
        The deployments to terminate, labels for the records that resolved to
        nothing -- already gone, so nothing to do -- and labels for the ones
        that could not be resolved *and* could still be live, which is what
        makes the run exit non-zero.

    A 404 from the point lookup means the deployment is already gone, the common
    case for a run that finished normally. Anything else -- a transport failure,
    an unknown kind -- goes in the third list: "could not tell" and "not there"
    must not read the same when the difference is a cluster billing.
    """
    from singlestoredb.exceptions import ManagementError

    found: List[Tuple[str, Any]] = []
    gone: List[str] = []
    unresolved: List[str] = []

    managers: Dict[str, Any] = {}

    def manager_for(version: str) -> Any:
        if version not in managers:
            managers[version] = _manager(version)
        return managers[version]

    for record in read_ledger(path):
        kind = record.get('kind')
        name = record.get('name')
        ident = record.get('id')
        label = '{} {} ({})'.format(
            str(kind).replace('_', ' '), name or '<unnamed>', ident or 'no id',
        )

        if kind not in LEDGER_KINDS:
            unresolved.append(f'{label}: unknown kind {kind!r}')
            continue
        version, lookup_name, listing = LEDGER_KINDS[kind]

        try:
            mgr = manager_for(version)
        except Exception as exc:
            unresolved.append(
                f'{label}: could not reach management API '
                f'{version}: {exc}',
            )
            continue

        obj = None
        try:
            if ident is not None:
                obj = getattr(mgr, lookup_name)(ident)
            else:
                # A `pending` record: the creator never returned an id, so the
                # only handle on it is the name. Matched over the listing
                # exactly as utils._recover_orphan does.
                for candidate in listing(mgr):
                    if getattr(candidate, 'name', None) == name:
                        obj = candidate
                        break
        except ManagementError as exc:
            if exc.errno == 404:
                gone.append(label)
                continue
            unresolved.append(f'{label}: {exc}')
            continue
        except Exception as exc:
            unresolved.append(f'{label}: {exc}')
            continue

        if obj is None:
            gone.append(label)
        elif getattr(obj, 'terminated_at', None) is not None:
            gone.append(f'{label} (already terminated)')
        else:
            found.append((label, obj))

    return found, gone, unresolved


def _run_ledger_sweep(path: str, yes: bool) -> int:
    """Report, and with ``yes`` terminate, everything the ledger still lists."""
    leftovers, gone, unresolved = find_ledger_leftovers(path)

    print(
        f'Ledger {path}: {len(leftovers)} still live, {len(gone)} already '
        f'gone, {len(unresolved)} unresolved.\n',
    )

    if unresolved:
        print(
            f'{len(unresolved)} ledger record(s) could not be resolved, so '
            'they may still be live:',
        )
        for label in unresolved:
            print(f'  ? {label}')
        print()

    if not leftovers:
        # Non-zero only for the records whose state is unknown: a clean run
        # whose sweep already terminated everything must not fail the job.
        print('Nothing left behind by this run.')
        return 1 if unresolved else 0

    print(f'{len(leftovers)} deployment(s) left behind by this run:')
    for label, _ in leftovers:
        print(f'  - {label}')

    if not yes:
        print('\nDry run; pass --yes to terminate these.')
        return 1 if unresolved else 0

    from singlestoredb.tests import utils

    failed = 0
    for label, obj in leftovers:
        try:
            utils.terminate(obj, timeout=TERMINATE_TIMEOUT)
        except Exception as exc:
            failed += 1
            print(f'✗ {label}: {exc}')
        else:
            print(f'✓ terminated {label}')

    return 1 if (failed or unresolved) else 0


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[1])
    parser.add_argument(
        '--yes', action='store_true',
        help='actually terminate; without this the run only reports',
    )
    parser.add_argument(
        '--ledger', metavar='PATH',
        help='sweep exactly what the run that wrote this JSONL ledger created '
             '(see SINGLESTOREDB_TEST_DEPLOYMENT_LOG). Replaces the name '
             'patterns and the age filter, which would spare everything in it '
             'for being minutes old. CI runs this as an if: always() step',
    )
    parser.add_argument(
        '--secrets', action='store_true',
        help='sweep stranded org-scoped secrets instead of deployments (see '
             'SECRET_PATTERNS). Honours --yes, --older-than, '
             f'--include-unknown-age and --show-unmatched; --older-than '
             f'defaults to {DEFAULT_SECRET_MIN_AGE_HOURS} here rather than '
             f'{DEFAULT_MIN_AGE_HOURS}, since a secret a live run owns is '
             'seconds old, not hours',
    )
    parser.add_argument(
        '--older-than', type=float, default=DEFAULT_MIN_AGE_HOURS,
        metavar='HOURS',
        help='only sweep deployments at least this old '
             f'(default: {DEFAULT_MIN_AGE_HOURS}). Pass 0 to sweep every '
             'match, which will terminate deployments a concurrent test run '
             'is still using',
    )
    parser.add_argument(
        '--since', type=parse_since, metavar='DATE',
        help="sweep what was created on or after DATE -- 'today', "
             "'yesterday' or YYYY-MM-DD, counted from local midnight -- "
             'instead of what is older than --older-than. This is for '
             'clearing out a recent session rather than reaping strays, so '
             'it removes the guard against terminating a deployment a live '
             'run owns: pair it with --kind',
    )
    parser.add_argument(
        '--any-name', action='store_true',
        help='consider every live deployment, not only the ones named like '
             "the test suite's. This will terminate deployments nothing in "
             'this repo created, including ones a colleague is using, so '
             'read the dry run first',
    )
    parser.add_argument(
        '--kind', action='append', choices=KINDS, dest='kinds',
        metavar='KIND',
        help='restrict the sweep to this kind of deployment; repeatable. '
             f'One of: {", ".join(KINDS)}. Defaults to all of them, which is '
             'rarely what you want alongside --any-name',
    )
    parser.add_argument(
        '--include-unknown-age', action='store_true',
        help='also sweep matches whose creation time the API did not report '
             '(skipped by default, since an unknown age can be shown neither '
             'to be old enough nor to fall after --since)',
    )
    parser.add_argument(
        '--show-unmatched', action='store_true',
        help='also list the live deployments this tool does not recognize as '
             "the suite's, without touching them. Run this when the "
             'organization looks full of strays but the sweep finds none: a '
             'test that names a deployment outside the conventions in '
             'PATTERNS is invisible here until its name is added',
    )
    args = parser.parse_args(argv)

    # --ledger asks "what did *this* run make?", not "what looks stranded?", so
    # it does not compose with the name and age guards. Erroring beats silently
    # ignoring them.
    if args.ledger:
        for flag, value in (
            ('--older-than', args.older_than != DEFAULT_MIN_AGE_HOURS),
            ('--since', args.since is not None),
            ('--any-name', args.any_name),
            ('--kind', bool(args.kinds)),
            ('--show-unmatched', args.show_unmatched),
            ('--secrets', args.secrets),
        ):
            if value:
                parser.error(f'{flag} does not apply with --ledger')
        return _run_ledger_sweep(args.ledger, args.yes)

    # A different subject, not a different filter: --secrets sweeps secrets
    # *instead of* deployments, so the flags that select deployments do not
    # compose with it either.
    if args.secrets:
        for flag, value in (
            ('--since', args.since is not None),
            ('--any-name', args.any_name),
            ('--kind', bool(args.kinds)),
        ):
            if value:
                parser.error(f'{flag} does not apply with --secrets')
        # An explicit --older-than 6 is indistinguishable from the default
        # here, which costs nothing: it is the value the caller asked for
        # either way.
        older_than = (
            DEFAULT_SECRET_MIN_AGE_HOURS
            if args.older_than == DEFAULT_MIN_AGE_HOURS
            else args.older_than
        )
        return _run_secret_sweep(
            older_than, args.include_unknown_age, args.yes,
            args.show_unmatched,
        )

    kinds = args.kinds or list(KINDS)

    leftovers, spared, unmatched = find_leftovers(
        args.older_than, args.include_unknown_age,
        since=args.since, any_name=args.any_name, kinds=kinds,
    )

    if args.since is not None:
        print(
            'Selecting {} created on or after {:%Y-%m-%d %H:%M %Z}, {}.\n'
            .format(
                '/'.join(kinds),
                args.since,
                'any name' if args.any_name
                else 'named like the test suite',
            ),
        )

    if args.show_unmatched:
        if unmatched:
            print(
                f'{len(unmatched)} live deployment(s) not recognized as the '
                "suite's, and so never swept:",
            )
            for label in sorted(unmatched):
                print(f'  ? {label}')
            print(
                '\nIf one of these was made by a test, add its name to '
                'PATTERNS in this module.\n',
            )
        else:
            print('Every live deployment is recognized by PATTERNS.\n')

    if spared:
        print(f'{len(spared)} match(es) left alone by the age filter:')
        for label in spared:
            print(f'  - {label}')
        print()

    subject = 'deployment' if args.any_name else 'test deployment'

    if not leftovers:
        print(f'No matching {subject}s found.')
        return 0

    print(f'{len(leftovers)} matching {subject}(s):')
    for label, _ in leftovers:
        print(f'  - {label}')

    if not args.yes:
        print('\nDry run; pass --yes to terminate these.')
        return 0

    from singlestoredb.tests import utils

    failed = 0
    for label, obj in leftovers:
        try:
            # Same budget as the ledger sweep: --since or --older-than 0 can
            # select a deployment that is still provisioning, and nothing runs
            # after this either.
            utils.terminate(obj, timeout=TERMINATE_TIMEOUT)
        except Exception as exc:
            failed += 1
            print(f'✗ {label}: {exc}')
        else:
            print(f'✓ terminated {label}')

    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
