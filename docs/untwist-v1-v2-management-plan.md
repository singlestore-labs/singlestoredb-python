# Untwist the v1/v2 management API split

> **Self-contained implementation plan.** Every fact needed to execute is inline —
> file paths, line numbers, current-state excerpts, and triage lists. No re-exploration
> of the codebase should be necessary. Repo: `/home/ksmith/src/singlestoredb-python`,
> branch `versioned-management-api` (27 commits ahead of `main`).

---

## 1. Context

The management API's v1→v2 change is not an ordinary revision: it **eliminates workspace
groups and workspaces in favor of a flat `Cluster` resource**. The current design tried to
*bridge* those two vocabularies, and the bridge is where all the complexity went:

- `management/versioned.py` (158 lines) implements `.v1`/`.v2` attribute switching via
  `__getattr__`, which forces every entity to stash `_response` in `from_dict`, plus
  `_version_map`, `_version_response`, `inspect.signature` sniffing of `from_dict`,
  `_location` copying, and region propagation.
- `management/v1/_translate.py` (106 lines) + `management/v1/cluster.py` (43 lines) exist
  **only** to serve that switching — renaming `workspaceID↔clusterID`,
  `workspaceGroupID↔groupID`, `kaiEnabled↔kai`, and folding/unfolding `size`/`scaleFactor`.
- `tests/test_versioned_management.py` has grown to **1791 lines — larger than
  `test_management_v1.py` (1524)** — and roughly half tests that plumbing, not real behavior.

Since v1 and the whole workspace-group concept are slated for deletion, this bridge is
throwaway complexity that makes the code harder to read *now* and buys nothing later.

**Intended outcome:** `Workspace`/`WorkspaceGroup` (v1) and `Cluster` (v2) become simply
separate classes with no bridge. Modules that differ only by URL stay shared; anything with
a real behavioral difference is reimplemented in its own version directory. Base classes are
level-set to speak v2, so `v1/` holds backward overrides and deleting `v1/` is a clean
`rm -rf`.

**Destination: everything moves to v2.** Keeping v1 working is a *gate*, not a permanent
requirement — it is how we confirm the restructure broke nothing before the default flips.
Every v1-specific thing this plan adds is deliberately **scaffolding** built to be deleted:
the backward overrides in `v1/`, the `'v1'` `default_version` literals, and the v1 test
suite. Part 7 is the flip, arranged to be a small obvious commit rather than a second
refactor.

## 2. Agreed rules

1. **No cross-version imports, either direction.** `v1/` must not import `v2/`; `v2/` must
   not import `v1/`.
2. **Shared only when the difference is the URL.** Any other difference → reimplement the
   class in the proper version directory.
3. **No `workspace_id` / `workspace_group_id` in cluster code.** Cluster classes use
   `cluster_id` / `group_id`.
4. **No runtime "am I a workspace or a cluster?" branching** in code or tests.
5. **Level-set the code to v2 now; flip the runtime default last.** `management.version`
   already ships on `main` with default `'v1'`. It stays `'v1'` through Parts 1-6 so the v1
   suite is a valid regression gate, then flips in Part 7.
6. **`job.py` stays shared** with version-specific target-type class attributes
   (explicitly decided; see Part 4).

## 3. Scope boundary

**Fusion cluster support is out of scope.** `fusion/handlers/utils.py` is hardwired to v1:
line 25 is `return manage_workspaces()`, lines 17-20 import
`StarterWorkspace`/`Workspace`/`WorkspaceGroup`/`WorkspaceManager` from
`...management.workspace`, and lines 106 and 173 raise
`'clusters and shared workspaces are not currently supported'` when
`SINGLESTOREDB_CLUSTER` is set — a branch that never fires, since no environment
sets that variable (see §4.3). There is **no cluster grammar** in `fusion/handlers/`
(files: `export.py`, `files.py`, `job.py`, `models.py`, `stage.py`, `utils.py`), so there is
nothing for a `test_fusion_v2.py` to exercise. `test_fusion.py` stays the v1 suite.
**This is the one thing blocking full v2 adoption**, so it is the natural next piece of work
after Part 7 — flagged, not solved here.

---

## 4. Current state reference

### 4.1 File inventory (`singlestoredb/management/`, 8,858 lines)

```
    9  __init__.py            public re-exports
   73  billing.py             shared, fully version-neutral
  152  billing_usage.py       shared, fully version-neutral
   76  cluster.py             v2-only shim + manage_clusters()
    6  export.py              v1-only shim
 1278  files.py               shared + manage_files();  identical at v1/v2
  363  inference_api.py       shared impl, but v1-only routes
  942  job.py                 shared
  385  manager.py             shared Manager base
  250  organization.py        shared
  180  region.py              shared + manage_regions()
  754  stage.py               shared
  530  utils.py               shared helpers
  158  versioned.py           THE MACHINERY
   73  workspace.py           v1-only shim + manage_workspaces()

    2  v1/__init__.py
  106  v1/_translate.py       v1<->v2 field renames
   43  v1/cluster.py          v1 "landing point" for v2 cluster bodies
  298  v1/export.py           real v1 impl (workspaceGroup-scoped)
   21  v1/files.py            pure re-export
   12  v1/inference_api.py    pure re-export
   23  v1/job.py              pure re-export
   10  v1/organization.py     pure re-export
   11  v1/region.py           pure re-export
   10  v1/billing_usage.py    pure re-export
 1502  v1/workspace.py        real v1 impl

    2  v2/__init__.py
 1132  v2/cluster.py          real v2 impl
  276  v2/export.py           real v2 impl (cluster-scoped, egress/*)
   21  v2/files.py            pure re-export
   52  v2/inference_api.py    subclass; every method raises
   38  v2/job.py              subclass; overrides 3 targetType attrs
   19  v2/organization.py     subclass; repoints 2 sub-manager classes
   41  v2/region.py           subclass; list_shared_tier_regions raises
   10  v2/billing_usage.py    pure re-export
```

Shape: `v1/`/`v2/` are **name namespaces**. Version-neutral implementations live in the
top-level modules; each version package either re-exports verbatim or subclasses to
override one or two attributes. `.flake8:19-24` blanket-ignores F401 for `v1/*.py` and
`v2/*.py` to permit the re-exports.

### 4.2 Where the version distinction is encoded

**Config option** — `singlestoredb/config.py:312-316`:
```python
register_option(
    'management.version', 'string', check_str, 'v1',
    'Specifies the version for the management API.',
    environ=['SINGLESTOREDB_MANAGEMENT_VERSION'],
)
```
Read in exactly **two** places, both at call time: `region.py:174` and `files.py:585`,
each `ver = version or config.get_option('management.version') or 'v1'`.

**`default_version` literals (4):** `manager.py:51` `'v1'`, `files.py:532` `'v1'`,
`v1/workspace.py:1160` `'v1'`, `v2/cluster.py:860` `'v2'`. These were changed from
option-reads to literals by commit 393570e1 — reading the option at import froze it and let
a v1 class declare itself v2. **Do not reintroduce option-reads here.**

**URL construction** — the single site, `manager.py:90-93`:
`urljoin(self._base_url_root, version or type(self).default_version) + '/'`. Version is a
path segment, not a separate host.

**Factories** (all call `_import_versioned_module(ver, <module>)`):
- `region.py:174-180` `manage_regions` — reads option
- `files.py:585-590` `manage_files` — reads option
- `workspace.py:62-73` `manage_workspaces` — **pins v1**, raises if `version != 'v1'`
- `cluster.py:65-76` `manage_clusters` — **pins v2** via `DEFAULT_CLUSTER_VERSION`, raises
  if `version == 'v1'`

**`_version_map` declarations (all in `v1/workspace.py`):**
- `:119` `Workspace` → `{'v2': ('cluster', 'Cluster')}`
- `:501` `WorkspaceGroup` → `{'v2': ('cluster', 'WorkspaceGroup')}` — **intentionally
  dangling**; `v2/cluster.py` defines no `WorkspaceGroup`, so `wg.v2` raises via
  `versioned.py:75-78`. Pure indirection for an error message.
- `:912` `StarterWorkspace` → `{'v2': ('cluster', 'StarterCluster')}`
- `:1170` `WorkspaceManager` → `{'v2': ('cluster', 'ClusterManager')}`

**`_version_response` overrides** — `v1/workspace.py:268-271` and `:996-999`. These are the
**only** `if version == ...` conditionals in the entire package.

**Path/route differences:**
- `v1/workspace.py:46` `SHAREDTIER_PATH = 'sharedtier/virtualWorkspaces'`
  vs `v2/cluster.py:51` `SHAREDTIER_PATH = 'sharedtier/virtualClusters'`
- `stage.py:70` → `stage/{id}/fs/{path}` vs `v2/cluster.py:67-68` →
  `clusters/{id}/stage/fs/{path}`
- `v1/export.py` workspaceGroup-scoped vs `v2/export.py:128` `_egress_path` under
  `clusters/{id}/egress/`

**Version-specific subclass overrides:**
- `v2/job.py:33-38` — `_deployment_target_type = TargetType.CLUSTER`,
  `_starter_target_type = TargetType.VIRTUAL_CLUSTER`, `_legacy_cluster_target_type = None`
- `v2/region.py:23-41` — `list_shared_tier_regions` raises
- `v2/inference_api.py:27-52` — all five methods raise `_NO_V2_ROUTE`
- `v2/organization.py:18-19` — swaps in v2 `JobsManager`/`InferenceAPIManager`

**Hook attributes:** `organization.py:137-138` `_jobs_manager_class` /
`_inference_api_manager_class`.

**`_response` writes (11 sites, read ONLY by the switching machinery — verified):**
`v1/workspace.py:265, 676, 993`; `v2/cluster.py:358, 705`; `files.py:130`;
`inference_api.py:208`; `job.py:651`; `organization.py:204`; `region.py:79`;
`billing_usage.py:91, 151`.

### 4.3 Vocabulary leakage

**Functional (code, not docstrings):**
- `utils.py:229-241` — `get_cluster_id()` → `SINGLESTOREDB_CLUSTER`,
  `get_workspace_id()` → `SINGLESTOREDB_WORKSPACE`,
  `get_virtual_workspace_id()` → `SINGLESTOREDB_VIRTUAL_WORKSPACE`
- `job.py:736-751` `_resolve_target` uses v1-named locals for both versions
- `job.py:77-80` — `TargetType.WORKSPACE` / `VIRTUAL_WORKSPACE` in the shared enum
- `job.py:715, 718` — shared defaults are the **v1** vocabulary
- `v2/cluster.py:57` — `CLUSTER_ENV_VARS = ('SINGLESTOREDB_CLUSTER', 'SINGLESTOREDB_WORKSPACE')`

**⚠ Correction to the above (established after Part 7).** `SINGLESTOREDB_CLUSTER`
**does not exist**: the notebook environment publishes the current deployment as
`SINGLESTOREDB_WORKSPACE` at every API version — a workspace ID at v1, a cluster
ID at v2 — plus `SINGLESTOREDB_WORKSPACE_GROUP` for the group ID and
`SINGLESTOREDB_PROJECT` for the project. So:
- `CLUSTER_ENV_VARS` collapses to `('SINGLESTOREDB_WORKSPACE',)`, and
  `get_cluster_id()` is simply the v2 spelling of `get_workspace_id()`. **Landed
  as a deletion:** a one-element tuple is not worth a name, so the constant is
  gone (removed in `3a9ebb04`) and every reader goes through
  `management/utils.py`'s `get_cluster_id()`.
- `SINGLESTOREDB_WORKSPACE_GROUP` is *not* a deployment variable. Its value is a
  group ID, which v2 reports only as the read-only `Cluster.group` and offers
  no route to look up, so `get_deployment()` refuses to guess which cluster was
  meant and raises pointing at `SINGLESTOREDB_WORKSPACE`. No constant names it:
  the one site that checks it (`fusion/handlers/utils.py`) never reads its
  value.
- The legacy self-managed cluster target is gone from the write path: nothing
  sets the variable that named it, so `_resolve_target` has only the starter and
  deployment branches.
- `v2/inference_api.py:22` — error string says `manage_workspaces(version='v1')`, i.e. v2
  code naming a v1 factory

**Verified already clean:** `v2/cluster.py` has **no** `workspace_id` or `workspaceGroupID`
identifiers. The only `workspace` hits in `v2/` are historical comments
(`v2/cluster.py:5,7,8,18`, `v2/job.py:24,25,29`) plus `v2/inference_api.py:22`. Rule 3 is
already satisfied for identifiers.

**Docstrings saying `WorkspaceManager` in shared modules that outlive v1:**
`region.py:17,21,29,61-62,92,96,127,137,161,165`;
`organization.py:121,125,141,190-191,214-215,230-231`;
`billing_usage.py:73-74,104,137-138`; `files.py:40,43`; `job.py:703-704`; `stage.py:44`;
`manager.py:339`; `utils.py:2` (module docstring wrongly reads
`"""SingleStoreDB Cluster Management."""`).

### 4.4 `v2/cluster.py` structure (for reference when writing tests)

- `class Stage(_Stage)` at `:60` — base is shared `management.stage.Stage`; sole body is
  `_fs_path` → `clusters/{id}/stage/fs/{path}` (`:67-68`).
- `class Cluster(VersionedMixin)` at `:119` — flat resource carrying the union of v1
  `Workspace` + `WorkspaceGroup` fields (`:141-168`): `group, size, scale_factor, state,
  created_at, terminated_at, expires_at, last_resumed_at, endpoint, provider, region,
  project_id, deployment_type, kai, multi_az, allow_all_traffic, firewall_ranges,
  outbound_allow_list, opt_in_preview_feature, update_window, auto_suspend, auto_scale,
  cache_config, resume_attachments, scaling_progress, smart_dr_status`.
  Methods: `from_dict` (`:305`), `_require_manager` (`:361`), `organization`/`stage`/
  `stages` (`:369-378`), `refresh` (`:380`), `update` (`:389`), `terminate` (`:474`),
  `connect` (`:515`), `suspend` (`:537`), `resume` (`:570`).
- `class StarterCluster(VersionedMixin)` at `:610` — `name, id, database_name, endpoint,
  mysql_dml_port, websocket_port, project_id`; `connect`, `terminate`, `refresh`,
  `organization`, `stage`, `create_user`.
- `class ClusterManager(Manager)` at `:837` — `default_version = 'v2'` (`:860`),
  `obj_type = 'cluster'` (`:867`). Properties: `clusters` (GET `clusters`, `:870`),
  `starter_clusters` (GET `SHAREDTIER_PATH`, `:876`), `organizations`, `organization`,
  `billing`, `regions` (`@ttl_property`, 1h). Methods: `create_cluster` (`:904`),
  `get_cluster` (`:1040`), `get_starter_cluster` (`:1057`),
  `create_starter_cluster` (`:1074`), `shared_tier_regions` (`:1120`).
- `create_cluster` params (`:904-928`): `name, region, provider, region_name, size,
  scale_factor, firewall_ranges, allow_all_traffic, admin_password, auto_suspend,
  auto_scale, cache_config, deployment_type, expires_at, update_window, kai, multi_az,
  opt_in_preview_feature, project_id, wait_on_active, wait_interval, wait_timeout`.
  One call replaces v1's `create_workspace_group` + `create_workspace`.
  **Its POST body was inferred from the GET response shape and never verified against the
  live API.**
- Module level: `SHAREDTIER_PATH` (`:51`), `CLUSTER_ENV_VARS` (`:57` — since
  deleted, see the correction in §4.3), `get_organization` (`:71`), `get_secret`
  (`:77`), `get_cluster` (`:82`), `get_stage` (`:112`).

### 4.5 Current test state

| File | Lines | Version-aware? |
|---|---|---|
| `tests/test_versioned_management.py` | 1791 | Yes — exclusively; 100% mock-based |
| `tests/test_management_v1.py` | 1524 | No — entirely v1 |
| `tests/test_fusion.py` | 1547 | No — entirely v1 |
| `tests/conftest.py` | 216 | No — Docker lifecycle only |

**There is no v1/v2 split in the integration tests at all** — zero version-parametrized
fixtures, zero `if version ==`, zero `is_cluster` predicates, zero version skip markers,
zero shared base test classes. All version content is quarantined in one mock-based file.

**`v2/cluster.py` has ZERO integration coverage.** Every live test builds workspace groups
via `manage_workspaces()`; nothing calls `manage_clusters()` against a real endpoint.

`test_management_v1.py` classes, all gated only by `@pytest.mark.management` (registered at
`pyproject.toml:93-94`): `:35 TestWorkspace` (→ `:44 manage_workspaces()`),
`:210 TestStarterWorkspace`, `:319 TestStage`, `:872 TestSecrets`, `:929 TestJob`,
`:1082 TestFileSpaces` (`manage_files()`), `:1418 TestRegions` (`manage_regions()`),
`:1491 TestRemotePathUtils` (pure unit).

Management tests require `SINGLESTOREDB_MANAGEMENT_TOKEN` against real cloud — **not** the
Docker image — so they skip locally.

---

## 5. Implementation

### Part 1 — Delete the cross-version bridge

Pure deletion, no replacement. Users reach a version through the factory they call.

1. **Delete** `management/v1/_translate.py` and `management/v1/cluster.py`.
2. **In `management/versioned.py`:** remove `VersionedMixin` entirely — `__getattr__`,
   `_get_versioned`, `_version_target`, `_version_response`, `_get_version_cache`,
   `_version_map`, `_version_cache`, `_response`. **Keep `_import_versioned_module`**
   (`:134-158`) and `_VERSION_RE` (`:15`); the factories still use them. The file drops
   from 158 → ~30 lines; rename it to reflect that it is now just the version-module
   importer (e.g. `_version_import.py`) and update the 5 import sites.
3. **Remove the mixin from its users:** `manager.py:44` `class Manager(VersionedMixin)` →
   `class Manager`; `v1/workspace.py:100, 478, 892`; `v2/cluster.py:119, 610`.
4. **Remove `_version_map`** at `v1/workspace.py:119, 501, 912, 1170`.
5. **Remove `_version_response`** at `v1/workspace.py:268-271, 996-999`, and the now-unused
   `_translate` imports at `v1/workspace.py:42-43`.
6. **Remove all 11 `out._response = obj` lines** listed in §4.2.
7. **Remove manager-cloning plumbing** in `manager.py:71-79` — `_base_url_root` and
   `_version_cache` existed for clones. Careful: `_base_url_root` is also used by
   `__init__`'s own URL construction at `:90-93`, so keep whatever that needs; delete only
   the clone-support state. Also drop `_is_jwt` propagation if it exists solely for clones
   (check `manager.py` `is_jwt`).

### Part 2 — Level-set base classes to v2

Today shared modules encode **v1** behavior and `v2/` overrides *forward*. Invert: base =
v2, `v1/` overrides *backward*. Mechanical, and the change that makes deleting `v1/` clean.

Per module: move the v1 value/method into a real subclass in `v1/`, promote the v2 value
into the shared base, reduce the `v2/` module to a pure re-export.

| Module | Base becomes (v2) | `v1/` gains |
|---|---|---|
| `stage.py` | `_fs_path` → `clusters/{id}/stage/fs/{path}` (from `v2/cluster.py:67-68`) | **new** `v1/stage.py`: `Stage._fs_path` → `stage/{id}/fs/{path}` (today `stage.py:52-70`) |
| `job.py` | `_deployment_target_type = TargetType.CLUSTER`, `_starter_target_type = TargetType.VIRTUAL_CLUSTER`, `_legacy_cluster_target_type = None` (`job.py:715,718,722`) | `v1/job.py` becomes a real subclass overriding those three back to `WORKSPACE`/`VIRTUAL_WORKSPACE`/`CLUSTER` |
| `region.py` | drop shared-tier support from the base (v2 raises today) | `v1/region.py` gains the real `list_shared_tier_regions` (today `region.py:127-137`) |
| `organization.py` | `_jobs_manager_class` / `_inference_api_manager_class` (`:137-138`) → v2 values | `v1/organization.py` becomes a real subclass repointing **both** to the v1 classes |
| `inference_api.py` | v2 has **no** inference routes — move the 363-line impl into `v1/inference_api.py`; stop exporting from `v2/` | `v1/inference_api.py` holds the implementation |

**Consequences to handle:**

- `v2/cluster.py:60` — the `Stage(_Stage)` subclass becomes unnecessary; delete it and
  import `Stage` from `..stage`.
- `v1/workspace.py` must import `Stage` from `.stage` (new file) instead of `..stage`.
- `v2/job.py`, `v2/organization.py`, `v2/region.py` reduce to pure re-exports.
- **Delete `v2/inference_api.py`** (52 lines of five raising methods). Not exporting the
  class is cleaner than exporting one that raises, and it removes `v2/inference_api.py:22`,
  which violates rule 1.
- `fusion/handlers/utils.py:15-16` imports `InferenceAPIInfo`/`InferenceAPIManager` from
  `...management.inference_api`. If the impl moves to `v1/`, **either** keep a top-level
  `inference_api.py` shim re-exporting v1 (consistent with `export.py`), **or** update the
  Fusion imports. Prefer the shim — Fusion is v1-only and this keeps Part 2 free of Fusion
  churn.
- **`TargetType` (`job.py:57-80`) stays a shared union enum.** The read path
  (`TargetType.from_str`) must round-trip either version's wire value without knowing which
  produced it. Only the write path is version-specific, via the three class attributes.
  Note `'Cluster'` means *different things* per version — legacy self-managed at v1, the v1
  "workspace" at v2 — which is exactly why the union is required. Only the read path ever
  sees the v1 sense: the write path takes its target from `SINGLESTOREDB_WORKSPACE`, which
  never names a legacy cluster.

**⚠ The sharp edge:** `v1/organization.py` is currently a 10-line pure re-export, so v1's
`Organization` picks up the shared base `JobsManager`. **If the base flips to v2 target
types without `v1/organization.py` repointing `_jobs_manager_class`, v1 job scheduling
silently starts sending v2 `targetType` values.** Same for `_inference_api_manager_class`.
Every backward override must land in the *same commit* as the base flip.

**Deliberate temporary exception:** `Manager.default_version` stays `'v1'`
(`manager.py:51`), as do `files.py:532` and `v1/workspace.py:1160`. Holding them at `'v1'`
is what keeps the v1 suite a valid regression gate — if defaults flipped in the same commit,
a broken override would be indistinguishable from an intended change. Mark all three with
an identical comment naming Part 7 so they are trivial to find.

**Top-level `export.py`** (6-line v1 shim) stays pointed at v1: `fusion/handlers/export.py:9-11`
imports `_get_exports`/`ExportService`/`ExportStatus` from it and Fusion is v1-only.
`v1/export.py` and `v2/export.py` are already fully separate and need no change.
This outlived the branch — see the annotation on Part 7's `export.py` bullet.

### Part 3 — Vocabulary cleanup

- **`job.py:736-751` `_resolve_target`** — rename the v1-flavored locals to neutral names
  (`starter_id`, `deployment_id`). Keep the `utils.py:229-241` env-var
  reader **names as-is**: they read `SINGLESTOREDB_WORKSPACE` etc., which is the notebook
  runtime's external contract, not ours to rename.
- **`v2/cluster.py:57` `CLUSTER_ENV_VARS`** — keep `SINGLESTOREDB_WORKSPACE`, and *only* it;
  same reason. Make the existing justification comment at `:53-56` say so plainly, including
  that no `SINGLESTOREDB_CLUSTER` exists to prefer over it.

  **Landed differently:** with one variable left, the constant was deleted
  outright rather than reduced to a one-element tuple, and the justification now
  lives on `management/utils.py`'s `get_cluster_id()` — the single accessor every
  reader in `management/` and `fusion/` goes through. The env-var *names* are
  still the notebook runtime's contract and unchanged.
- **Docstring sweep** — replace `WorkspaceManager` with `ClusterManager` and drop
  workspace-group phrasing at every site listed in §4.3.
- **`utils.py:2`** — fix the module docstring.

### Part 4 — Make `manage_clusters()` the front door

- Add a `DeprecationWarning` to `manage_workspaces()` (`workspace.py:22`) pointing at
  `manage_clusters()`. Behavior otherwise identical: still pinned to v1, still raises on an
  explicit non-v1 `version=`.
- **Do not warn on internal use.** `fusion/handlers/utils.py:25` calls
  `manage_workspaces()` on **every Fusion command**, and Fusion is v1-only by design. Add a
  module-level `_manage_workspaces_v1()` holding the current body; `manage_workspaces()`
  warns then delegates; Fusion calls the private one.
- `management/__init__.py` (9 lines) currently exports `get_organization`, `get_secret`,
  `get_stage`, `manage_workspaces` from `.workspace`. Add `manage_clusters` and list it
  first.
- **Done differently, and further:** re-exporting the three `get_*` helpers from
  `.workspace` left neutral names bound to v1 implementations that ignore
  `management.version` and disappear with the v1 package. They are now version-neutral
  functions of their own — `get_organization`/`get_secret` in `management/organization.py`,
  `get_stage` in `management/stage.py` — dispatching through
  `_versioned_attr()` to whichever version the option resolves to; each version package
  re-exports its own from `__init__.py`. `manage_workspaces()` follows the option too and
  raises for a non-v1 resolution, mirroring `manage_clusters()`; the pinned behavior lives
  on in the private `_manage_workspaces_v1()` that Fusion and the other v1-only internals
  call. The version-locked helpers stay reachable through the shims
  (`management.workspace.get_stage` is v1's, `management.cluster.get_stage` is v2's). See
  rule 2 in ADR 0001. Consequence: once the Part 7 flip sets the option to v2, a bare
  `manage_workspaces()` raises instead of returning a v1 manager, so every remaining
  workspace call site must pass `version='v1'` or move to clusters — the v1 and Fusion
  suites already pin theirs.
- Check `singlestoredb/__init__.py` for the same export set.
- Update `resources/create_test_cluster.py` (188 lines) and `resources/drop_test_cluster.py`
  (52 lines), which use `manage_workspaces` under cluster-sounding filenames — they will
  start emitting the new warning.

### Part 5 — Tests

Layout (flat files, no new directories, no packaging churn — `pyproject.toml:86` uses
`packages.find` auto-discovery, and flat files avoid needing `__init__.py` for the
`--pyargs singlestoredb.tests` invocation):

```
singlestoredb/tests/
  test_management_v1.py         # v1 suite — RENAMED from test_management.py
  test_management_v2.py         # NEW — cluster suite
  test_management_utils.py      # NEW — version-neutral unit tests
  test_management_versioning.py # NEW — small; factory pinning + v1-deletability
  test_fusion.py                # v1 — unchanged (see §3)
  test_versioned_management.py  # DELETED
```

The v1 suite keeps its scope but not its name: `test_management.py` was the only
one of the four without a version suffix, so it read like the umbrella suite
when it is version-specific — its own docstring already said "v1 Management API
testing". `test_management_v1.py` also makes the Part 8 deletion an unambiguous
file removal. Nothing in CI names the file (the workflows run the whole
`singlestoredb/tests` directory), so the rename was a `git mv` plus these docs.
Line counts and line numbers quoted elsewhere in this document predate the
rename and the Part 5 restructure; they are historical.

**Triage of `test_versioned_management.py`'s 29 classes — delete the file after:**

*→ `test_management_utils.py`* (zero version content; they live in the versioned file only
because that is where the bugs were found):
`TestFolderTransferPaths` (`:1408`, 13 tests), `TestRecursiveDownloadPathTraversal`
(`:1329`), `TestDateTimeParsingFixes` (`:652`), `TestSecretFromDictTimestamps` (`:1038`).
Move `TestRemotePathUtils` (`test_management_v1.py:1491`) here too for cohesion.

*→ `test_management_v1.py`* (real v1 behavior): `TestWorkspaceFromDictNewFields` (`:735`),
`TestWorkspaceUpdatePosting` (`:789`), `TestWorkspaceGroupNewFields` (`:841`),
`TestWorkspaceGroupCreateUpdatePosting` (`:904`), `TestJobsManagerScheduleDuration`
(`:958`), `TestTokenStorageFix` (`:533`). Also `TestWorkspaceGroupRegionResolution`
(`:1120`) — **keep** the 4-way region fallback ladder and its 4 tests (match by id →
name+provider → payload fields → `'<unknown>'`), but drop the "regions arriving from a v2
manager" framing, which disappears with the bridge.

*→ `test_management_v2.py`*: `TestV2RegionBehavior` (`:1080`).

*→ consolidate into `test_management_versioning.py`* (~150 lines, the only versioning tests
worth keeping): `TestImportVersionedModule` (`:137`, error messages),
`TestConfigOption` (`:392`), `TestManageRoutingForAllFactories` (`:1202`),
`TestFactoriesAreNotDuplicated` (`:1291`), `TestV1IsDeletable` (`:1694`).
**Extend `TestV1IsDeletable` to check both directions** — its AST walk over `v2/*.py` for
`ImportFrom` nodes (handling relative `level=2, module='v1.x'` and absolute forms) and its
`sys.meta_path` import blocker (`:1758-1786`) currently only assert "v2 must not import
v1". Mirror both for `v1/` → `v2/` so they enforce rule 1.
Note `TestConfigOption` currently restores with `original or 'v1'`, silently rewriting a
`None`/`''` original — fix while moving. (`conftest.py:180 protect_singlestoredb_url`
protects `SINGLESTOREDB_URL` but **not** `management.version`.)

*Delete with the machinery they test:* `TestVersionedMixin` (`:91`),
`TestManagerVersionSwitching` (`:174`), `TestEntityVersionSwitching` (`:232`),
`TestWrapperManagerVersionSwitching` (`:477`), `TestLocationManagerRebind` (`:560`),
`TestJWTRefreshInClones` (`:602`), `TestEntityRoundTripFidelity` (`:693`),
`TestV2InheritanceModel` (`:349`), `TestNoSilentFallback` (`:369`),
`TestModuleNameConvention` (`:462`), `TestTopLevelShims` (`:295`). Also delete the
`_MultiPatch` (`:53-66`) and `_patch_no_network_regions` (`:39-50`) helpers, whose only
purpose was patching two unrelated class hierarchies at once.
(`TestLocationManagerRebind` and `TestJWTRefreshInClones` cite commit SHAs `0cc6024f` /
`d52e8e40` that no longer exist in `git log` — rebased away. No loss.)

**`test_management_v2.py` — new coverage.** Port the shape of `test_management_v1.py`'s classes
to cluster vocabulary against `manage_clusters()`: `TestCluster`, `TestStarterCluster`,
`TestStage`, `TestSecrets`, `TestJob`, `TestRegions`, plus the rescued
`TestV2RegionBehavior`. Gate with `@pytest.mark.management` like the v1 suite. Use §4.4 for
the API surface. **These will skip locally** and **cannot be verified against a live v2
endpoint** as part of this work — flag every assertion that depends on an unverified payload
shape, especially anything touching `create_cluster`'s POST body.

**No test may branch on version.** Each file targets exactly one version.

### Part 6 — Docs

`docs/adr/0001-versioned-management-api-wrappers.md` (92 lines) drives the current design
and must be **amended or superseded**, not tweaked. It explicitly **rejected** "separate,
unrelated manager classes per version" on duplication grounds (`:75-77`) — that is the
decision being reversed. Update: "Version switching via VersionedMixin" (`:41-46`),
"Convention-based module lookup" (`:48-55`), "Response storage" (`:65-67`), the rejected
alternative (`:75-77`), and Consequences (`:87-92`). Also fix the **already-stale** claim at
`:63` that `default_version` is resolved from `config.get_option('management.version')` —
commit 393570e1 made those literals.

Record the new rules: separate classes per vocabulary; shared modules only for URL-only
differences; no cross-version imports; base level-set to v2.

### Part 7 — Flip the default to v2 — **landed**

**Gated on the v1 suite passing green after Part 2** (the "see v1 working first"
checkpoint). Deliberately small, because Parts 1-6 did the structural work:

- `config.py:313` — `management.version` default `'v1'` → `'v2'`.
- `manager.py:51` and `files.py:532` `default_version` → `'v2'`. **Leave
  `v1/workspace.py:1160` at `'v1'`** — `WorkspaceManager` is a v1 class and pinning it is
  correct; it disappears with `v1/`.
- `manage_files()` (`files.py:585`) and `manage_regions()` (`region.py:174`) then resolve to
  `/v2/` by default. **This is the only user-visible behavior change on the branch** — needs
  a `docs/whatsnew` entry.
- Top-level `export.py` repoints to `v2/export.py` **only after** Fusion cluster support
  lands (see §3). Until then it stays v1.

  **Still v1, and deliberately so.** Fusion cluster support has landed, but the
  gate was the wrong one: what blocks the repoint is not `CLUSTER` commands
  existing, it is the **EXPORT** grammar. `fusion/handlers/export.py` resolves
  its target with `get_workspace_group({})` at every call site, and v2's
  `ExportService.__init__` and `_get_exports` both take a `Cluster` — a
  `WorkspaceGroup` has no `/clusters/{id}/egress/*` route behind it. Repointing
  the shim would break every EXPORT handler with no v2 replacement to move them
  to. The real precondition is porting the EXPORT Fusion grammar to clusters,
  which is not on this branch. **Open.**

**As landed**, with two additions the plan did not anticipate:

- `_version_import.DEFAULT_VERSION` (`'v1'` → `'v2'`) had to flip with the option. It is the
  fallback when the option is *explicitly blanked*, not when it is merely unset, so leaving it
  at `'v1'` would have made `management.version=''` mean something different from the default.
  Consequence: a bare `manage_workspaces()` now raises and points at `manage_clusters()`,
  where before it returned a v1 manager.
- The v1 coverage is gated by a `management_v1` pytest marker rather than being deleted:
  module-level `pytestmark` in `tests/test_management_v1.py` plus `TestWorkspaceFusion` in
  `tests/test_fusion.py`. `-m 'not management_v1'` for a normal run, `-m 'management_v1'` for
  the nightly that keeps proving v1 works. The marker is deliberately separate from
  `management` because `test_management_v1.py` also holds mocked units that need no token —
  those are v1-specific too, and go away with `management/v1/`.
- `docs/src/whatsnew.rst` is generated at release time by `/bump-version` from the git log,
  so there is no hand-written entry. **Confirmed as the policy** — nothing on this branch
  touches `whatsnew.rst`. That puts the burden on the release commit messages, so here is
  the full list of user-visible breaks they have to carry:

  1. `manage_files()` and `manage_regions()` resolve to `/v2/` by default.
  2. `management.version` defaults to `'v2'`: a bare `manage_workspaces()` is deprecated and
     needs an explicit `version='v1'`, and `manage_clusters()` raises `ManagementError` if
     the option is pinned to `v1`.
  3. `manage_cluster` (singular, the legacy self-managed cluster entry point) is **removed**
     from `singlestoredb/__init__.py`'s exports. Zero remaining references in the repo.
  4. `Portal.cluster_id` returns `self.workspace_id` rather than reading
     `_connection_info['cluster']` / `SINGLESTOREDB_CLUSTER`; new `Portal.project_id`.
  5. `TTLProperty.reset()` → `reset(obj)`, needed to invalidate the new per-instance cache.
     No callers in the library, so this only matters if anything downstream used it.

- `docs/src/api.rst` now has a cluster section covering `manage_clusters`,
  `ClusterManager`, `Cluster`, `StarterCluster` and `Project`, and the workspace half is
  retitled "Workspaces (v1)" with a deprecation note. `management.timing` is deliberately
  left undocumented: it is internal. **Done.**

Then, as a **separate follow-up commit** once v2 is confirmed against a live endpoint:
delete `management/v1/`, `management/workspace.py`, `tests/test_management_v1.py`, and
`test_fusion.py`'s workspace grammar — i.e. everything the `management_v1` marker now
selects. Verification step 6 rehearses exactly this, so it should be mechanical.

---

## 6. Suggested commit order

1. Part 1 — pure deletion of the bridge
2. Part 3 — docs/naming, no behavior change
3. Part 2 — **one module at a time**, each with its `v1/` backward override in the same commit
4. Part 4 — `manage_clusters()` front door + Fusion private path
5. Part 5 — test restructure
6. Part 6 — ADR amendment
7. **v1 suite green** ← the gate
8. Part 7 — flip defaults

---

## 7. Verification

1. **Structural invariants** — `pytest -v singlestoredb/tests/test_management_versioning.py`.
   The AST scan plus `sys.meta_path` blocker must prove `v1/` and `v2/` do not import each
   other **in either direction**.
2. **v1 unchanged** — `pytest -v -m management singlestoredb/tests/test_management_v1.py` with
   `SINGLESTOREDB_MANAGEMENT_TOKEN` set. This is the real regression gate for Part 2. Watch
   job scheduling specifically: a missed `_jobs_manager_class` repoint sends v2 `targetType`
   values on v1.
3. **Version-neutral units** — `pytest -v singlestoredb/tests/test_management_utils.py`
   (needs no token, no container).
4. **Fusion not broken** — `pytest -v singlestoredb/tests/test_fusion.py`, plus
   `pytest -W error::DeprecationWarning singlestoredb/tests/test_fusion.py` to confirm the
   Part 4 internal path emits no warning.
5. **Grep gates:**
   - `rg -n "workspace" singlestoredb/management/v2/` → comments only, plus the deliberate
     `SINGLESTOREDB_WORKSPACE` env-var contract
   - `rg -n "_version_map|_version_response|VersionedMixin|_response\s*=" singlestoredb/management/`
     → no hits
   - `rg -n "if version ==" singlestoredb/management/` → no hits
6. **Deletability rehearsal** — `git rm -r singlestoredb/management/v1/`, delete
   `management/workspace.py` and `management/export.py`, then confirm `import singlestoredb`
   and `pytest singlestoredb/tests/test_management_v2.py --collect-only` still work.
   **Then revert; do not commit.** This is the real measure of whether the untwisting worked.
7. **Pre-commit** — `pre-commit run --all-files` (mandatory; flake8 / autopep8 /
   reorder-python-imports / add-trailing-comma / mypy). `.flake8:19-24`'s F401 exemption for
   `v1/*.py` and `v2/*.py` should still be needed for the remaining re-export modules.
8. **Full suite** — `pytest -v singlestoredb/tests` (Docker container auto-starts when
   `SINGLESTOREDB_URL` is unset).

## 8. Risks

- **Part 2 is the only behavior-risky change.** Every backward override in `v1/` must land
  in the same commit as its base flip, or v1 silently changes behavior. The
  `_jobs_manager_class` repoint is the specific trap.
- **v1 is a gate, not a deliverable.** The scaffolding added to `v1/` is written to be
  deleted, so it is not worth polishing. The cost to watch is the *opposite* failure: Part 2
  quietly leaving v1 behavior in a shared base, which would survive the `v1/` deletion and
  become a v2 bug long after the v1 tests are gone. Verification step 6 is the check.
- **v2 remains unverified.** Moving `inference_api.py` into `v1/` asserts v2 has no
  inference routes; dropping shared-tier from the `region.py` base asserts the same for
  shared-tier regions. Both are inferred from the existing raising subclasses
  (`v2/inference_api.py:27-52`, `v2/region.py:23-41`), **not** from the live API. If either
  is wrong, v2 loses a working route.
- **`create_cluster`'s POST body was never verified** against the live API (per commit
  01626a60). Any v2 test asserting on it is asserting on a guess.
- **`manage_cluster` (singular, legacy self-managed clusters) was already removed** on this
  branch in commit e3e33f8a. Anyone upgrading loses that name while gaining
  `manage_clusters` (plural) with entirely different semantics — needs a `docs/whatsnew`
  note.
