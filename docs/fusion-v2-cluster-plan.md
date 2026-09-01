# Fusion SQL: add v2 cluster support

## Context

Branch `versioned-management-api` has completed Parts 1–6 of
`docs/untwist-v1-v2-management-plan.md`: the management API is split into
version-neutral top-level modules whose base classes are level-set to **v2**,
with `management/v1/` holding backward overrides. Part 7 — flipping the
`management.version` default from `'v1'` to `'v2'` — was deliberately not done
when this plan was written. **It has since landed**, together with a
`management_v1` pytest marker that gates the v1 coverage so it can be demoted to
a nightly run; the Fusion work below is what unblocked it. The context that
follows describes the pre-flip state.

The one thing blocking that flip is Fusion. The plan's §8 names it: Fusion has no
cluster grammar and is hardwired to v1 at a single chokepoint,
`singlestoredb/fusion/handlers/utils.py:23-25`, which returns
`_manage_workspaces_v1()`. All 45 registered handlers funnel through that or its
sibling `get_files_manager()`.

The v2 object model is a different shape, not a rename:

- v1 has two levels — `WorkspaceGroup` containing `Workspace`, created in two
  calls. v2 has **one flat `Cluster`** (`management/v2/cluster.py:115`) carrying
  the union of both field sets, created in **one** `create_cluster()` call.
- `POST /v2/clusters` **requires** `projectID`, which `POST /v1/workspaceGroups`
  assigned implicitly. `Project` (`management/v2/project.py:25`) is data-only.
- v2 regions have no region ID, so `IN REGION ID '<id>'` cannot work at v2.

So this is not a flag flip: it means adding a cluster vocabulary alongside the
workspace vocabulary, and moving the non-deployment handlers onto v2 so `Cluster`
is verified to interoperate everywhere `WorkspaceGroup` did.

### Live API probe — findings (2026-08-24)

At the time of the probe, the OpenAPI dump at `dev-docs/management_api.openapi`
was version 1.1.124 with 42 v1 paths and only 2 v2 paths (`/v2/regions` and one
metrics route). It could not answer v2 questions, which is why the audit
repeatedly says "not in the spec dump." So the live API was probed read-only
instead:

> **Since then (2026-08-25)** the dump has been replaced with the current
> upstream spec (1.2.171, 65 paths). It now covers v2 properly, but publishes
> only nine v1 routes, so the v1 shapes cited elsewhere in these plans are no
> longer in the file. `egress` is still absent at both versions.

**The v2 sweep is safe.** Every route the swept handlers call answers at v2:
`/v2/organizations/current`, `/v2/jobs/runtimes`, `/v2/secrets`,
`/v2/files/fs/{personal,shared,models}` all 200; `/v2/clusters/{id}/stage/fs`
returns `400 uuid: incorrect UUID length` (route exists, bad ID). This resolves
the two largest unknowns — `/v2/jobs` and `/v2/files` were assumed, not verified.

**Two bugs in the current branch:**

1. `GET /v2/regions/sharedtier` returns **200** with
   `[{"region": "US East 1 (N. Virginia)", "provider": "AWS", "regionName": "us-east-1"}]`,
   identical to v1. But `management/region.py:124-146` and
   `v2/cluster.py:1363-1375` both raise `ManagementError` asserting it 404s and
   "no alternate spelling responds." Wrong, including the docstrings.
2. Live `GET /v2/regions` gives `region` = display name, `regionName` = provider
   slug — so `Region.from_dict` is correct, and the mock at
   `test_management_v2.py:120` (`region: 'us-east-1', regionName: 'US East 1'`)
   is reversed. It asserts the opposite of reality and would mask a regression.

**Region sense differs by route.** On `/v2/regions`, `region` is the display
name. On `/v2/clusters`, a cluster's own `region` is the **provider slug**
(`{"region": "us-east1", "provider": "GCP"}`). `CREATE CLUSTER` must therefore
resolve a display name to `regionName` before posting.

**Projects.** The org has three auto-provisioned projects — `Shared Project`
(SHARED), `Standard Project` (STANDARD), `Enterprise Project` (ENTERPRISE), all
sharing a `createdAt`. Existing clusters sit in `Standard Project`.
`_resolve_project_id()` only auto-resolves when there is exactly one, so it
raises here.

**Not verified:** whether `POST`/`PATCH /v2/clusters` honors `adminPassword`.
That needs a billable write, so it is an explicit step below rather than a
plan-mode probe.

### Decisions taken

1. **Both vocabularies coexist.** Workspace handlers stay pinned to v1 via
   `_manage_workspaces_v1()` and must keep passing untouched. New cluster
   handlers use a v2 `ClusterManager`. Both grammars register at once, keeping
   the v1 path exercised until `management/v1/` is deleted.
2. **Project: use the single project when there is one, else require
   `IN PROJECT`.** `_resolve_project_id()` already implements the first half and
   raises naming the candidates; the grammar gets an *optional* `IN PROJECT`
   clause that becomes necessary only in a multi-project org. Projects are never
   created or dropped from Fusion; `SHOW PROJECTS` exists so they can be
   discovered.
3. **Full v2 sweep** of stage, files and jobs. **Models/inference cannot move** —
   `Organization.inference_apis` raises for every version past v1
   (`management/organization.py:281-287`) and `management/inference_api.py` is
   v1-pinned, so `get_inference_api_manager()` stays on the v1 manager.
4. Full cluster surface including `USE CLUSTER` and starter clusters.
5. Fix both bugs found above, and probe the password behaviour for real.

## Step 0 — commit the work already on disk

24 files staged, nothing unstaged, management suites passing. Lands as one
commit: the v2 cluster/project implementation, `_version_import` machinery, and
the test split. This plan document is untracked and should be staged with it.

```bash
git status --short     # verify: all staged, nothing unstaged
pre-commit run         # re-stage and re-run until clean
git commit
```

## Step 1 — fix the two probe-confirmed bugs

- `singlestoredb/management/region.py:124-146` — replace the raising
  `list_shared_tier_regions` with the real implementation (`GET regions/sharedtier`,
  same shape as `list_regions`); the v1 override in `v1/region.py:8-16` becomes
  redundant and should be removed so the base serves both.
- `singlestoredb/management/v2/cluster.py:1363-1375` — same for
  `shared_tier_regions`; return `NamedList[Region]` like `regions` (`:944`).
- `singlestoredb/tests/test_management_v2.py:120` — correct the mock to the live
  shape: `{'provider': 'AWS', 'region': 'US East 1 (N. Virginia)', 'regionName': 'us-east-1'}`.
- `docs/management-api-audit.md` — correct the shared-tier-region finding; it
  currently records the 404 that does not happen.

Verify: `pytest singlestoredb/tests/test_management_v2.py -k region -v`, plus a
live `manage_clusters(version='v2').shared_tier_regions` returning one row.

## Step 2 — `fusion/handlers/utils.py`: a v2 accessor and resolvers

`get_workspace_manager()` stays v1 — `workspace.py` and `job.py` both use it, and
only `job.py` moves. Add alongside it:

- `get_cluster_manager() -> ClusterManager` → `manage_clusters(version='v2')`,
  pinned for the same reason `get_workspace_manager()` is pinned to v1: the
  cluster vocabulary *is* the v2 vocabulary and must not follow
  `management.version` out of v2.
- `get_cluster(params)` — mirrors `get_workspace()` (`:111-176`): name filters
  `manager.clusters`, raising `KeyError` on none and `ValueError` on ambiguity;
  ID uses `manager.get_cluster()` mapping `errno == 404` to `KeyError`; then the
  environment, through `management/utils.py`'s `get_cluster_id()`. That reads
  `SINGLESTOREDB_WORKSPACE`, which is what the notebook environment publishes
  the current deployment as at every version — a cluster ID from v2 onward.
  **Landed** as one accessor call rather than a list of env-var names: there is
  only ever one variable, so a `CLUSTER_ENV_VARS` tuple would have been new
  surface for no gain.
- `get_starter_cluster(params)` — same shape against `starter_clusters` /
  `get_starter_cluster()`.
- `get_project(params)` — resolves an `IN PROJECT` clause by name against
  `manager.projects` or by ID via `get_project()`, falling back to
  `management/utils.py`'s `get_project_id()` (`SINGLESTOREDB_PROJECT`, set by
  the notebook environment and holding either a name or an ID — told apart by
  `PROJECT_ID_RE`) and
  returning `None` when neither names a project so `create_cluster` falls
  through to `_resolve_project_id()`.
- `get_deployment(params)` — **repointed in place** to v2. Verified safe:
  `stage.py` is its only consumer, so the workspace handlers are unaffected.
  `workspace_groups`→`clusters`, `starter_workspaces`→`starter_clusters`,
  `get_workspace_group`→`get_cluster`, `get_starter_workspace`→`get_starter_cluster`;
  the two env branches collapse into a single `get_cluster_id()` read trying
  cluster then starter cluster on 404. Keep the `params['group']` keys wired so
  the existing `IN GROUP` spelling still parses as a synonym.
  `SINGLESTOREDB_WORKSPACE_GROUP`, if set and nothing else matched, raises a
  `KeyError` pointing at `SINGLESTOREDB_WORKSPACE` — its value is a group ID,
  which v2 reports only as the read-only `Cluster.group` and offers no route
  to look up, so silently resolving it could target the wrong deployment.
- `get_files_manager()` → `manage_files(version='v2')` (step 6).
- Drop the two stale raises at `:105-106` and `:172-173`. They claim clusters
  "are not currently supported" and were reworded to point at the `CLUSTER`
  commands, but they keyed off `SINGLESTOREDB_CLUSTER`, which no environment
  ever sets — dead branches.
- `get_inference_api_manager()` (`:329-332`) stays on `get_workspace_manager()`,
  with a comment explaining why this one is pinned while files/jobs are not.

Do **not** add a leaner `Stage(id, manager)` shortcut: it accepts nonexistent IDs
and turns errors into raw 404s from `clusters/{id}/stage/fs`, losing the
"no deployment found with ID" messages the tests assert.

## Step 3 — new `fusion/handlers/cluster.py`

A new file, auto-registered by `fusion/__init__.py:9-11`. Keeping it separate
from `workspace.py` avoids mixing a v1-pinned and a v2-pinned manager in one
module, and makes deleting the v1 surface an `rm` later.

Handlers, following the docstring-grammar style of `handlers/workspace.py`:

| Handler | Command |
|---|---|
| `ShowClustersHandler` | `SHOW CLUSTERS [<like>] [<extended>] [<order-by>] [<limit>]` |
| `ShowClusterRegionsHandler` | `SHOW CLUSTER REGIONS [<like>] [<order-by>] [<limit>]` |
| `ShowProjectsHandler` | `SHOW PROJECTS [<like>] [<order-by>] [<limit>]` |
| `CreateClusterHandler` | `CREATE CLUSTER [IF NOT EXISTS] name IN REGION r [IN PROJECT p] WITH SIZE s ...` |
| `SuspendClusterHandler` | `SUSPEND CLUSTER c [WAIT ON SUSPENDED]` |
| `ResumeClusterHandler` | `RESUME CLUSTER c [DISABLE AUTO SUSPEND] [WAIT ON RESUMED]` |
| `DropClusterHandler` | `DROP CLUSTER [IF EXISTS] c [WAIT ON TERMINATED] [FORCE]` |
| `UseClusterHandler` | `USE CLUSTER c [WITH DATABASE d]` |
| `ShowStarterClustersHandler` | `SHOW STARTER CLUSTERS [<like>] [<extended>] [<order-by>] [<limit>]` |
| `CreateStarterClusterHandler` | `CREATE STARTER CLUSTER [IF NOT EXISTS] n WITH DATABASE d IN REGION r WITH PROVIDER p` |
| `DropStarterClusterHandler` | `DROP STARTER CLUSTER [IF EXISTS] c` |

Each ends with `<Class>.register(overwrite=True)`.

Grammar constraints, verified in `fusion/handler.py`:

- **Never register a bare two-word `SHOW CLUSTER`** — `register_handler`
  (`registry.py:45-48`) matches longest-key-first, so it would swallow the
  engine's real `SHOW CLUSTER STATUS`. Every cluster SHOW is plural
  (`SHOW CLUSTERS`) or three-plus words (`SHOW CLUSTER REGIONS`).
- `CREATE CLUSTER IDENTITY` already exists in `export.py` (all handlers
  `_enabled = False`). It is the longer key so routing is correct if hidden
  handlers are ever enabled.
- Consecutive `] [` optionals are rewritten into an order-independent union
  (`handler.py:449`), as with `CREATE WORKSPACE GROUP`.

`CREATE CLUSTER` clauses map onto `create_cluster()` (`v2/cluster.py:1110`):
`IN REGION` (+ optional `WITH PROVIDER` to disambiguate), `IN PROJECT`,
`WITH SIZE`, `WITH SCALE FACTOR`, `AUTO SUSPEND AFTER ... WITH TYPE ...`,
`ENABLE KAI`, `WITH CACHE CONFIG`, `WITH FIREWALL RANGES`, `ALLOW ALL TRAFFIC`,
`WITH UPDATE WINDOW`, `EXPIRES AT`, `WAIT ON ACTIVE`. Reuse
`CreateWorkspaceHandler.run`'s auto-suspend seconds table
(`workspace.py:620-633`) and `CreateWorkspaceGroupHandler.run`'s update-window
split (`:498-501`).

**Revised 2026-08-28: no `WITH DEPLOYMENT TYPE` and no `ENABLE MULTI AZ`.** Both
shipped in the first cut and were removed. The clause list is meant to stop at
what `CREATE WORKSPACE GROUP` and `CREATE WORKSPACE` between them expose, so
that a v1 script has a v2 counterpart for everything it says; `deploymentType`
and `multiAZ` have no v1 counterpart. Every other v2-only clause here earns its
place: `WITH PROVIDER` replaces the missing `IN REGION ID`, `IN PROJECT` is
required by `POST /v2/clusters`, and `WITH SCALE FACTOR` is the other half of
`sizeConfig`. Both dropped options remain on
`ClusterManager.create_cluster`.

**No `WITH PASSWORD`** — step 4's probe ran and settled it: neither `POST` nor
`PATCH /v2/clusters/{id}` honours `adminPassword`, so there is no way to
implement the clause. **No region-ID alternate** — v2 has
none. Region resolution matches on both `.name` and `.region_name`, requires
`WITH PROVIDER` to break ties, and passes an unmatched literal straight through.

Columns: `SHOW CLUSTERS` → `Name`, `ID`, `Region`, `Size`, `State`; extended adds
`Provider`, `Endpoint`, `DeploymentType`, `FirewallRanges`, `ProjectID`,
`CreatedAt`, `TerminatedAt`. Report `x.region.region_name` — `Cluster.region` is
a `Region`, whose `name` is the display name and `region_name` the provider slug.
`SHOW CLUSTER REGIONS` → `Name`, `Provider`, `RegionName` (no `ID`, since
v2 has none). `SHOW PROJECTS` → `Name`, `ID`, `Edition`, `CreatedAt`.

`SHOW REGIONS` (`workspace.py:148`) is **left alone on v1** so its `ID` column
keeps working; `SHOW CLUSTER REGIONS` is the v2-native replacement.

`USE CLUSTER` mirrors `UseWorkspaceHandler` (`workspace.py:16`) but flat — no
`IN GROUP`, so it sets `portal.workspace = <id>` or the 2-tuple with a database.
Flagged risk: `singlestoredb.notebook.portal`'s contract is v1-shaped and cannot
be tested outside a Helios notebook.

## Step 4 — probe the password behaviour, then decide `WITH PASSWORD` (done)

> **The probe ran on 2026-08-25** against one throwaway `S-00`
> (`probe-adminpw-1787666027`, since terminated). Results in
> `docs/management-api-audit.md` items 8 and 14:
>
> 1. `POST` returns a **generated** password, re-confirmed by connecting with
>    both values — the one sent is refused `1045`, the one returned works.
> 2. `PATCH /v2/clusters/{id}` **does not honour `adminPassword`** either. It is
>    accepted, the cluster reports ACTIVE, and the original generated password
>    keeps working — the same accept-and-silently-ignore shape audit item 9
>    records for `name`.
>
> **Outcome: `WITH PASSWORD` is not implementable and is not offered.**
> Create-then-PATCH was the only route and it does not work. The audit entries
> are the upstream bug report. `CREATE CLUSTER` returns the one-row result
> described below, which is the only place the generated password appears.

Create one throwaway `S-00` cluster and settle what the audit could not:

1. `POST /v2/clusters` with a known `adminPassword` — does the create response
   return that value or a generated one? (Audit finding 8 says generated,
   confirmed 2026-08-21; re-confirm since everything else in the audit's v2
   assertions has now had one error.)
2. `PATCH /v2/clusters/{id}` with `adminPassword` — then attempt a real
   connection with it. Necessary because audit finding 9 records that PATCH
   *accepts and silently ignores* `name`, so acceptance proves nothing.
3. Terminate the cluster; record both results in `docs/management-api-audit.md`.

Outcome drives the grammar:
- PATCH honors it → add `WITH PASSWORD '<password>'` as create-then-PATCH.
- PATCH ignores it → omit the clause; the audit entry becomes the upstream bug
  report. ← **this is what happened.**

Either way `CREATE CLUSTER` returns a one-row result carrying `Name`, `ID`,
`Endpoint`, `AdminPassword` from `Cluster.admin_password` (`v2/cluster.py:297`) —
the generated password appears in the create response and nowhere else, so
without this a Fusion-created cluster is unreachable. This diverges from
`CREATE WORKSPACE GROUP` returning `None`; note it in the docstring.

Also record in the audit the four v1 workspace-group capabilities with no v2
equivalent: `adminPassword` (ignored), `backupBucketKMSKeyID`,
`dataBucketKMSKeyID`, `smartDR`. `highAvailabilityTwoZones` survives, renamed to
`multiAZ` (`v2/cluster.py:250`). No KMS or `SMART DR` clauses on `CREATE CLUSTER`
— they would be silently dropped.

## Step 5 — `stage.py`: add `IN CLUSTER`

`get_deployment` is already repointed by step 2, so this is grammar only. Add to
each of the six handlers' `in` alternation:

```
in = { in_cluster | in_group | in_deployment }
in_cluster = IN CLUSTER { deployment_id | deployment_name }
in_group = IN GROUP { deployment_id | deployment_name }
in_deployment = IN { deployment_id | deployment_name }
```

Order matters — alternation is first-match, so `in_cluster` and `in_group` must
precede the bare `in_deployment` or `IN CLUSTER 'x'` parses as a deployment named
`CLUSTER`. This is why `IN GROUP` already precedes `IN` today.

Interop is low-risk: `stage.py` only touches `deployment.stage.{listdir,info,
upload_file,download_file,remove,removedirs,rmdir,mkdir}`, and `Cluster.stage`
(`v2/cluster.py:388`) returns `Stage(self.id, manager)` routing through
`clusters/{id}/stage/fs/{path}` — confirmed live. Nothing reads `.size`,
`.workspaces` or `.region` off a deployment. `StarterCluster.stage` is
documented-broken at both versions, so tests must not point at one.

## Step 6 — files to v2 (separate commit)

`get_files_manager()` → `manage_files(version='v2')`. `v1/files.py` and
`v2/files.py` are pure re-exports, so the only difference is the URL prefix — and
all three `/v2/files/fs/*` spaces returned 200 in the probe. Own commit so it is
trivially revertible.

## Step 7 — jobs to v2 (separate commit)

Eight call sites in `job.py`: `get_workspace_manager().organizations.current.jobs`
→ `get_cluster_manager()...`. `/v2/jobs/runtimes` and `/v2/organizations/current`
both 200, so the routes exist. This is still a wire-format change:
`targetConfig.targetType` goes from `Workspace`/`VirtualWorkspace` to
`Cluster`/`VirtualCluster` (`job.py:713,716`), which the probe could not exercise
without scheduling a job. Own commit, separate from step 6, so a jobs failure and
a stage failure stay distinguishable.

## Step 8 — tests

**`TestFusion`** (no token, runs in CI under `-m 'not management'`) — the cheap
regression net:
- registry contains the new commands; `SHOW FUSION GRAMMAR FOR "create cluster"`
  renders and contains neither `REGION ID` nor a KMS clause
- `registry.get_handler('SHOW CLUSTER STATUS')` is `None` — guards the
  two-word-key mistake
- `REGION ID` still present in `CREATE WORKSPACE GROUP`'s syntax, absent from
  `CREATE CLUSTER`'s
- a representative maximal `CREATE CLUSTER` statement parses

**`TestClusterFusion`** (`@pytest.mark.management`) — mirrors
`TestWorkspaceFusion` but flat. `setUpClass` uses `s2.manage_clusters(version='v2')`,
skips if no projects or no US regions (borrow the skip logic at
`test_management_v2.py:63`), and creates `A/B/C Fusion Cluster Testing {id}` at
`S-00` so the `LIKE`/`ORDER BY`/`LIMIT` assertions copy over. `tearDownClass`
terminates each in `try/except` plus a `_wait_cluster_gone` poller, mirroring the
existing `_wait_workspace_group_gone`. Note `POST /v2/clusters` enforces
`[a-z0-9]([a-z0-9-]*[a-z0-9])?` at 1–32 chars (audit finding 7), so names must be
lowercase and hyphenated — not the spaced names the workspace tests use.

Coverage: `SHOW CLUSTERS` (bare/`LIKE`/`ORDER BY`/`LIMIT`/`EXTENDED`),
`SHOW PROJECTS`, `SHOW CLUSTER REGIONS` (asserts `RegionName` populated and no
`ID` column — doubles as the live check on region shape), create/drop by name and
by ID plus `IF EXISTS`/`IF NOT EXISTS`, suspend/resume, `IN PROJECT` both named
and omitted, and `IN REGION ID 'x'` failing to parse.

**Switched suites:** `TestStageFusion` (`:756`) and `TestFilesFusion` (`:1288`) to
`s2.manage_clusters(version='v2')`, with stage setup creating a cluster via
`create_cluster(..., wait_on_active=True)`; add `IN CLUSTER` variants beside the
existing `IN GROUP` ones. `TestJobsFusion` (`:507`) likewise, in the step-7
commit. **`TestWorkspaceFusion` is not touched** — decision 1. Switch rather than
duplicate; the v1 stage path is already covered by `test_management_v1.py`, and
duplicating doubles a suite that already runs for tens of minutes.

## Verification

```bash
# no token needed
pytest singlestoredb/tests/test_fusion.py -m 'not management' -v

# registry wiring — expect 45 -> ~56 commands, none missing.
#
# The estimate was exact: 45 on main, 56 once the 11 cluster commands landed.
# The registry now holds 48, because a later commit (0b0765f5) hid the eight
# inference and MODEL commands. So: 48, none of the 11 missing, zero MODEL.
python -c "
from singlestoredb.fusion import registry
want={'SHOW CLUSTERS','SHOW CLUSTER REGIONS','SHOW PROJECTS','CREATE CLUSTER',
      'DROP CLUSTER','SUSPEND CLUSTER','RESUME CLUSTER','USE CLUSTER',
      'SHOW STARTER CLUSTERS','CREATE STARTER CLUSTER','DROP STARTER CLUSTER'}
print('missing:', want - set(registry._handlers), '| total:', len(registry._handlers))"

# routing, incl. the engine-shadowing guard (must print None)
SINGLESTOREDB_FUSION_ENABLED=1 python -c "
from singlestoredb.fusion import registry as r
for q in ['SHOW CLUSTERS','SHOW CLUSTER REGIONS','SHOW CLUSTER STATUS','SHOW PROJECTS']:
    print(repr(q),'->',getattr(r.get_handler(q),'__name__',None))"

# get_deployment really moved
python -c "
import inspect; from singlestoredb.fusion.handlers import utils
s=inspect.getsource(utils.get_deployment)
assert 'workspace_groups' not in s and 'clusters' in s; print('ok')"

# live, with a token — one suite per step so failures stay attributable
pytest singlestoredb/tests/test_fusion.py -k ClusterFusion -v
pytest singlestoredb/tests/test_fusion.py -k StageFusion -v
pytest singlestoredb/tests/test_fusion.py -k FilesFusion -v
pytest singlestoredb/tests/test_fusion.py -k JobsFusion -v
pytest singlestoredb/tests/test_fusion.py -k WorkspaceFusion -v   # must be unchanged

# full regression + lint
pytest singlestoredb/tests/test_fusion.py -v
pytest singlestoredb/tests/test_management_v1.py \
       singlestoredb/tests/test_management_v2.py \
       singlestoredb/tests/test_management_versioning.py -v
pre-commit run --all-files
```

## Risks

- **`create_cluster`'s POST body has never been sent live** — `test_management_v2.py`
  mocks `_post`. `TestClusterFusion` and step 4 are its first real exercise of
  `projectID`, `sizeConfig: {size, scaleFactor}`, `multiAZ`, `updateWindow`,
  `deploymentType`. Expect iteration.
- **`USE CLUSTER` is a coin flip.** `notebook.portal` takes v1-shaped
  `(group_id, workspace_name)` tuples; whether it accepts a v2 cluster ID is not
  determinable from this repo and not testable outside Helios. Ship it, but expect
  it may need revisiting.
- **Jobs target-type change is untested by the probe.** Routes exist; the
  `Cluster`/`VirtualCluster` `targetType` vocabulary is exercised only by
  scheduling a real job in `TestJobsFusion`.
- **`DROP CLUSTER FORCE`** passes `force` as a query param (`v2/cluster.py:539`).
  At v1 it meant "even if it has workspaces"; a v2 cluster has no children, so
  the semantics are unclear and possibly ignored. Confirm during step 4's probe,
  and drop the clause if it is a no-op.

  **Dropped.** `DROP CLUSTER` offers no `FORCE` clause. The probe did not
  establish what `force` means at v2 — `DELETE /v2/clusters` still takes the
  query parameter, and `Cluster.terminate()` documents it as "even if it is in
  use", which is a different meaning from v1's "even if it has workspaces" and
  is unconfirmed. Withheld rather than guessed at; the reasoning is in the
  handler's docstring (`fusion/handlers/cluster.py:685-692`).
- **Cost and duration.** `TestClusterFusion` creates three real clusters plus a
  suspend/resume cycle, making it the slowest test in the repo. Consider reusing
  one cluster across the read-only SHOW tests.
- **The audit is not fully trustworthy** — this planning pass already found one
  false assertion in it (shared-tier regions). Re-verify rather than cite it.
