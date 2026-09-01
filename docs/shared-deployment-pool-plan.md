# Sharing deployments across the management test suites

## Goal

Cut the serial wall time of the management suite by deploying fewer clusters,
not by polling faster. Measured target: **~1300s off an 8915s run (~15%)**, with
no change to what is asserted.

## Why this is the lever

A traced run (`SINGLESTOREDB_MANAGEMENT_TRACE=1`) spends 8874s of its 8915s
elapsed inside the management API -- 3729s in requests, 5145s asleep in
`wait_on_*` loops. There is no local work to optimize. Waiting for an S-00
cluster to reach ACTIVE costs ~460s and is irreducible, so the only serial
lever is **deploying fewer of them**.

Per-class fixture cost from that run:

| fixture | cost | deploys |
| --- | --- | --- |
| `test_fusion.py::TestStageFusion` | 891s | 2 clusters |
| `test_fusion.py::TestJobsFusion` | 686s | 1 cluster |
| `test_management_v2.py::TestJob` | 366s | 1 cluster |
| `test_management_v2.py::TestStage` | 247s | 1 cluster |
| **total** | **2190s** | **5 clusters** |

All four need is *a* live cluster. `TestStageFusion` needs two, because it
exercises `IN GROUP '<other cluster>'`; two therefore covers all four classes.

Pool cost is one 2-cluster deployment, ~890s (which is what `TestStageFusion`
measures today for exactly that). **2190s -> ~890s.**

## Audit: why these four are safe to share

Established by reading every assertion in each class:

* Every Stage assertion is scoped to one deployment's filesystem, and every
  path is namespaced with the class's `cls.id`, so two classes on one cluster
  cannot see each other's files.
* Job listings are filtered by job id (`show jobs {job_id} like ...`), never by
  a bare listing of the deployment's jobs.
* None of the four asserts a row count over an org-wide listing.

### Must NOT join the pool

* `test_management_v2.py::TestCluster` (125s) and
  `test_management_v1.py::TestWorkspace` (243s) -- these test the deployment
  objects themselves. `TestCluster::test_update` PATCHes the cluster and cycles
  it back through PENDING; a pooled cluster would break every other class.
* `TestClusterFusionCreateDrop`, `TestClusterFusionSuspendResume` -- both mutate
  or destroy their subject by definition.
* `TestWorkspaceFusion` -- its three workspace groups are the subject of its
  `SHOW WORKSPACE GROUPS` assertions, and it deploys them without waiting
  (39.7s total), so there is nothing to save.

## The one real implementation hazard

`conftest.py::pytest_runtest_setup` sets a per-class owner and sweeps the
previous owner's deployments as soon as the run moves to the next class:

```python
utils.set_owner(owner)
if previous:
    _sweep_live_deployments(previous)     # -> cleanup_tracked(previous)
```

`install_deployment_tracking()` (conftest.py:78) patches the creation methods,
so a pooled cluster built inside a `setUpClass` is tracked under *that class*
and **terminated the moment the run leaves it**. Naively hoisting the fixture
therefore produces a pool that dies after its first consumer.

`cleanup_tracked` matches on `x[0] == owner` (utils.py:493), and
`pytest_unconfigure` calls it with `owner=None`, which matches everything. So
an entry tracked under the empty owner survives every per-class sweep and is
terminated exactly once, at session end. Create the pool with the owner
temporarily cleared:

```python
prev = utils.get_owner()
utils.set_owner('')      # session-owned: no per-class sweep matches ''
try:
    cluster = mgr.create_cluster(...)
finally:
    utils.set_owner(prev)
```

No change to `conftest.py` or the sweep is needed -- this uses the existing
mechanism as designed.

## Steps

1. **Add the pool helper** in `singlestoredb/tests/utils.py`: a lazily built,
   process-wide pool of N v2 clusters with the owner-clearing block above.
   Cache on a module global; return the same objects on every call. Have it
   `raise unittest.SkipTest` for the same reasons the current fixtures do (no
   US regions, no STANDARD project), so skip behaviour is unchanged.
   *Verify:* a unit test that calls it twice and asserts the same cluster ids
   come back, and that the tracked entry's owner is `''`.

2. **Move `TestStageFusion` onto the pool** (`cls.cluster`, `cls.cluster_2`).
   It already needs exactly two. Drop the cluster creation and the
   `terminate(force=True)` calls from its `tearDownClass`; keep the env-var
   save/restore and the `load_sql`/`drop_database` calls, which are per-class
   and cheap (local server, not the pool cluster).
   *Verify:* `pytest -v singlestoredb/tests/test_fusion.py::TestStageFusion`
   passes and the trace shows no `POST clusters`.

3. **Move `TestJobsFusion`, `test_management_v2.py::TestStage` and
   `test_management_v2.py::TestJob` onto pool cluster 0.** Same edit shape.
   *Verify:* each class passes standalone, then all four pooled classes pass in
   one run -- that ordering is what proves the sweep does not eat the pool.

4. **Re-run with `SINGLESTOREDB_MANAGEMENT_TRACE=1`** and confirm
   `POST clusters` drops by 3 and the four fixtures total ~890s instead of
   2190s.

## Optional follow-ups, in value order

* **Deploy the two pool clusters concurrently** (two threads in the pool
  builder, joined before returning). Pool cost ~890s -> ~460s, another ~430s.
  Self-contained: two threads inside one fixture, not test-level parallelism.
* **Check whether the v1 suites can share the pool too.** `Cluster` carries a
  `group` attribute (`v2/cluster.py:163`), so a v2 cluster may be addressable
  as a v1 workspace group -- v1 Stage is keyed `stage/{id}/fs` where v2 is
  `clusters/{id}/stage/fs`. If `cluster.group` works as that id, then
  `test_management_v1.py::TestStage` and `::TestJob` (340s, deploys a group
  *and* a workspace) could join, worth another ~350s. **Verify before
  designing for it** -- this is a hypothesis, not a known fact.
* **`test_management_v1.py::TestStage` sharing `TestJob`'s workspace group.**
  Only ~15-25s, since it creates a group without waiting on it. Low priority.

## Out of scope

Test-level parallelism (pytest-xdist / concurrent class execution). The pool is
a prerequisite for it but independent of it: every number above is a serial
saving. Note that a process-wide pool and xdist interact -- under xdist each
worker builds its own pool, so the saving is per worker, not per session.

### Follow-up: what parallelism needs from the pool

Since taken up. The pool is process-wide, so which worker a borrowing class
lands on decides how many pools get built. Four borrowers spread over four
workers is four pools -- ~890s apiece, and the saving above is gone.

The borrowers therefore carry `xdist_group` marks
(`utils.SHARED_CLUSTER_STAGE_GROUP`, `utils.SHARED_CLUSTER_JOBS_GROUP`) and the
suite runs under `--dist loadgroup`. Two groups rather than one, split by what
they borrow:

| group | classes | pool |
| --- | --- | --- |
| `shared-cluster-stage` | `TestStageFusion`, v2 `TestStage` | 2 clusters |
| `shared-cluster-jobs` | `TestJobsFusion`, v2 `TestJob` | 1 cluster |

One group would serialise all four classes behind a single pool build. Two run
concurrently on separate workers, so the extra pool costs one cluster and no
wall time -- the builds overlap -- and halves the chain. The marks are inert
without `-n`: one process, one pool of two, exactly the serial behaviour above.

`--dist loadgroup` is load-bearing beyond the groups. xdist's default `--dist
load` distributes individual tests, so a unittest class is split across workers
and each one runs `setUpClass` itself: one cluster fixture becomes N
deployments. That is slower and more expensive than running serially. Because
forgetting it costs money, it is not left to the invocation: `addopts` in
`pyproject.toml` sets `-n 3 --dist loadgroup` for every run. The command line is
applied after `addopts`, so `-n 0` still gives a serial run and an explicit
`--dist` still wins. 3 rather than `auto` because the ceiling is the API's
tolerance for concurrent provisioning, not the host's CPUs.

One thing parallelism does not fix, and three it breaks:

* `TestClusterFusionCreateDrop::test_create_drop_cluster` is a single test of
  most of twenty minutes. One test cannot be split, so ~1200s is the floor on
  wall time whatever `-n` is.
* Peak concurrent deployments rises even though total cluster-hours does not --
  the two pools, `TestClusterFusion`'s three, `CreateDrop`, `TestCluster`, v1's
  group plus workspace, and both starter deployments can all be in flight at
  once. The org's cluster quota and the shared-tier starter limit are what cap
  `-n`, not anything in the tests.

  The API misbehaves under that burst. A cross-process cap on in-flight
  creations was tried and removed: `utils.deployment_slot()` held one of
  `SINGLESTOREDB_TEST_DEPLOY_CONCURRENCY` (default 3) `flock`ed slot files for
  the whole `create_*` call. It did not fix the failures it was aimed at, and it
  added wall time to every parallel run, so it is gone. Nothing bounds
  concurrent provisioning now -- `-n` is capped by the org's cluster quota and
  the shared-tier starter limit, and by whatever the API tolerates. **Open.**
* UNVERIFIED: `USE_DATA_API=1` with `-n` may not be safe on a shared
  container. `load_sql` ends with `SET GLOBAL HTTP_PROXY_PORT` and
  `RESTART PROXY` (`utils.py:227`), which every worker runs, and a restart
  while another worker has an HTTP request in flight would drop it. The MySQL
  path does not reach that branch. Not hit yet -- the parallel runs so far have
  been over the MySQL protocol.
* `SINGLESTOREDB_MANAGEMENT_TRACE`'s terminal summary is lost: `conftest.py`
  accumulates the traces in module globals filled in the workers, and
  `pytest_terminal_summary` runs in the controller, which sees none of them.
  The per-event stderr log still works. Measure with a serial run.
