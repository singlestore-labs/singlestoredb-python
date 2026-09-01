# Plan: don't return a cluster until it is truly usable

Branch: `versioned-management-api`. All work is in the v2 management wrappers
plus the live v2 test suite. Nothing here touches v1 behavior.

> **Status: all six steps landed.** Each step below carries a note saying where.
> Step 6 was the one item held for a decision; it was taken and shipped, and the
> snippet it proposed is not quite what went in — see that step.

## Background (all verified live against a real org, 2026-08-21)

`POST /v2/clusters` applies `firewallRanges` **asynchronously and outside the
state machine**. A cluster created with `firewallRanges: ['0.0.0.0/0']` reaches
`ACTIVE` with a resolvable endpoint while `GET /v2/clusters/{id}` still reports
`firewallRanges: []`. Empty means deny-all, so a connection attempt in that
window times out at the TCP level rather than failing authentication.

`create_cluster(wait_on_active=True)` therefore does **not** deliver a usable
cluster today:

- `_wait_on_state(out, 'ACTIVE')` returns as soon as the state flips.
- `_wait_on_endpoint()` (`management/manager.py:325`) returns immediately unless
  `SINGLESTOREDB_WORKLOAD_TYPE` is set, i.e. it is a no-op outside the notebook
  environment — so outside notebooks there is no endpoint check at all.

How long the gap lasts varies: one live run had the firewall in place by the
time the tests ran, the next did not. That non-determinism is what made
`TestCluster::test_connect` flaky.

`PATCH /v2/clusters/{id}` is asynchronous the same way — after a PATCH with new
`firewallRanges`, the immediately following `GET` still reports the old ranges
while the cluster cycles through `PENDING`, so the trailing `refresh()` inside
`Cluster.update()` reliably reports stale values. `update()` has no `wait_on_*`
parameters at all.

Recorded as items 9 and 12 in `docs/management-api-audit.md`.

## Scope

1. `create_cluster()` waits for the firewall as part of `wait_on_active`.
2. `Cluster.update()` gains opt-in waiting.
3. The live suite stops polling for the firewall itself and instead asserts the
   SDK did it.
4. Pin the v1 suite's two env-following `manage_*` calls to `v1`.
5. Decide what `manage_clusters()` should default to.

Out of scope: `create_starter_cluster()` (the shared-tier route has no firewall
field — the payload is `name`, `databaseName`, `provider`, `regionName`), the
notebook-only gate on `_wait_on_endpoint`, and the v1 workspace-group firewall
path.

---

## Step 1 — `ClusterManager._wait_on_firewall()`

New private method on `ClusterManager` in `singlestoredb/management/v2/cluster.py`.

Put it in `v2/cluster.py`, **not** `management/manager.py`: this is a v2 API
quirk, and keeping it out of the shared base means zero risk to the v1
workspace path. Per ADR 0001, version-specific behavior belongs in the version
package.

Signature, mirroring the existing wait helpers:

```python
def _wait_on_firewall(self, out, interval=10, timeout=600) -> 'Cluster':
```

Behavior:

- Poll `get_cluster(out.id)` until `out.firewall_ranges` is non-empty.
- On timeout raise `ManagementError` naming the cluster, the elapsed wait, and
  the fact that the endpoint will refuse all inbound connections — the same
  shape as `_wait_on_state`'s timeout message.
- Return the refreshed `Cluster`.

**Wait for non-empty, not for set-equality with the requested ranges.** The
server may normalize what it stores, and `allow_all_traffic=True` has no
documented on-the-wire representation to compare against, so equality would be
guessing. Non-empty is the property that actually matters: it is the difference
between deny-all and reachable. Record this reasoning in the docstring.

Verify: unit test that a mocked `get_cluster` returning `[]`, `[]`,
`['0.0.0.0/0']` causes exactly three calls and returns the third object.

**Landed** as `ClusterManager._wait_on_firewall` (`v2/cluster.py:1063`), waiting
on non-empty as described.

## Step 2 — call it from `create_cluster()`

In `create_cluster()` (`v2/cluster.py:985`), inside the existing
`if wait_on_active:` block, after `_wait_on_state` and `_wait_on_endpoint`:

```python
if firewall_ranges or allow_all_traffic:
    out = self._wait_on_firewall(out, interval=wait_interval, timeout=wait_timeout)
```

Gating rules, both deliberate:

- Only when a firewall was actually requested. `firewall_ranges=[]` is a
  legitimate deny-all request (audit item 5: the field must be present, `[]`
  disallows all inbound traffic) and must not hang for ten minutes waiting for
  a non-empty value that is never coming.
- Only under `wait_on_active`. A caller passing `wait_on_active=False` has
  opted out of waiting; do not silently reintroduce a block.

**The `out._admin_password = body.get('adminPassword')` assignment must stay
after every wait.** Each wait re-fetches the cluster, and `refresh()`/
`get_cluster()` produce an object whose `_admin_password` is `None`; the
generated password exists only in the create response. Getting this order wrong
loses admin access to the cluster and the existing unit test
`test_create_cluster_returns_the_generated_admin_password` is what catches it.

Update the `wait_on_active` docstring to say what is waited on (state, then
endpoint, then firewall) and why the firewall is included.

Verify:
- Unit: `wait_on_active=True` + `firewall_ranges=['0.0.0.0/0']` polls until
  non-empty.
- Unit: `firewall_ranges=[]` and `firewall_ranges=None` do **not** poll.
- Unit: `wait_on_active=False` does not poll.
- Unit: the existing admin-password test still passes (order regression).
- `pytest singlestoredb/tests/test_management_v2.py -q -m 'not management'`

**Landed** in `create_cluster` (`v2/cluster.py:1440`), with both gates and the
admin-password ordering as written.

## Step 3 — opt-in waiting on `Cluster.update()`

Add to `update()` (`v2/cluster.py:400`), after the existing keyword arguments:

```python
wait_on_active: bool = False,
wait_interval: int = 10,
wait_timeout: int = 600,
```

Default `False` to keep the current signature backward compatible. When true,
after the `PATCH`: wait for `ACTIVE`, then wait on the firewall if
`firewall_ranges or allow_all_traffic` was passed, then `refresh()`.

Note in the docstring that without this the trailing `refresh()` reports
pre-PATCH values, because the API applies the change asynchronously.

Verify: unit test that `update(firewall_ranges=[...], wait_on_active=True)`
polls and that `update(firewall_ranges=[...])` does not.

**Landed** on `Cluster.update` (`v2/cluster.py:473`), the three keywords
defaulting as written.

## Step 4 — simplify the live suite

In `singlestoredb/tests/test_management_v2.py`:

- Delete the module-level `_wait_for_firewall()` helper and its call in
  `TestCluster.setUpClass`. `create_cluster(wait_on_active=True,
  firewall_ranges=['0.0.0.0/0'])` must now deliver this itself.
- Add to `setUpClass`, right after the create, a plain assertion that
  `cls.cluster.firewall_ranges` is non-empty. Cheap, no polling, and it is now
  a real regression test of step 2 rather than a workaround.
- In `test_update`, replace the 30-iteration polling loop with the new
  `wait_on_active=True` argument, so the test exercises step 3.
- Keep the existing assertion that `name` is silently ignored by the PATCH
  route (audit item 9).
- `time` may become an unused import — check.

Verify: `pytest "singlestoredb/tests/test_management_v2.py::TestCluster" -m
management` — 8 tests, roughly 4 minutes, creates and terminates one real
cluster. `test_connect` passing here is the whole point: it is the test that
was timing out at the TCP level.

**Landed.** `_wait_for_firewall` is gone from `test_management_v2.py`.

## Step 5 — pin the v1 suite's env-following `manage_*` calls

The factories are already correct: `manage_files()` (`management/files.py:558`)
and `manage_regions()` (`management/region.py:149`) both take `version` and
default to `config.get_option('management.version') or 'v1'`, i.e. the
environment setting. Leave that alone — it is the wanted behavior.

The problem is two call sites in the **v1** suite that follow the environment
and so will silently start testing v2 when the default flips in Part 7:

- `singlestoredb/tests/test_management_v1.py:1100` — `s2.manage_files()`
- `singlestoredb/tests/test_management_v1.py:1436` — `s2.manage_regions()`

Pass `version='v1'` at both. The other five `manage_workspaces()` calls in that
file need nothing: `manage_workspaces()` is v1-locked by the factory, which
raises if any other version is requested.

The v2 suite is already explicit where it matters
(`manage_regions(version='v2')` at `test_management_v2.py:1106`).

Verify: `SINGLESTOREDB_MANAGEMENT_VERSION=v2 pytest
singlestoredb/tests/test_management_v1.py -q -m 'not management'` — the v1 unit
tests must be unaffected by the env var.

**Landed** at `test_management_v1.py:1110` and `:1448`, each with a comment
saying why the pin is there. This is now the project-wide rule: a test pins the
version it means rather than inheriting the ambient option.

## Step 6 — `manage_clusters()`'s default (decided and shipped)

`manage_clusters()` currently ignores `management.version` entirely and uses
`DEFAULT_CLUSTER_VERSION = 'v2'` (`management/cluster.py:29`). That conflicts
with "manage_* should default to the environment setting", but it cannot simply
follow the option either: the option still defaults to `'v1'` until the Part 7
flip, and `manage_clusters()` raises `ManagementError` for `v1` because
clusters do not exist there.

Recommendation: follow `management.version` **when that version has clusters**,
otherwise fall back to `DEFAULT_CLUSTER_VERSION`:

```python
ver = version or config.get_option('management.version')
if not ver or ver == 'v1':
    ver = DEFAULT_CLUSTER_VERSION
```

This keeps today's behavior identical (option is `v1` → `v2` is used), stops
pinning the front door to v2 forever, and means a future `v3` is picked up by
the environment without another code change. The explicit-`version='v1'` error
path stays as-is, since that is a caller asking for something that does not
exist rather than an ambient default.

**Decided as recommended, and landed — but not with the snippet above.** The
option now defaults to `'v2'` (the Part 7 flip), so there is no longer a `v1`
default to step around, and the whole resolution collapses into the shared
helper every other entry point uses:

```python
ver = _resolve_version(version, default=DEFAULT_CLUSTER_VERSION)
if ver == 'v1':
    raise ManagementError(...)
```

`management/cluster.py:72`. `DEFAULT_CLUSTER_VERSION` survives as the fallback
for an *explicitly blanked* option, which is the same role
`_version_import.DEFAULT_VERSION` plays for `manage_workspaces()`. The `v1` →
`ManagementError` path is unchanged, and it now fires for an option-supplied
`v1` as well as an explicit argument — which is the intended reading of "clusters
do not exist in v1", not a regression.

Verify: unit tests for all four cases — no option set, option `v1`, option
`v2`, explicit `version='v1'` still raising.

---

## Wrap-up

- `pre-commit run --files <changed>` until clean (mandatory).
- Update `docs/management-api-audit.md` items 9 and 12 to record what was
  fixed in the wrapper versus what remains an API-side bug worth raising with
  the API team. Both underlying API behaviors are still bugs; the SDK is only
  papering over them.
- Do **not** claim the suite passes without a live run. The full v2 suite takes
  over an hour; `TestCluster` alone (~4 min) covers everything this plan
  touches.
