# Review of the `versioned-management-api` branch

Read-only review of the whole branch (60 commits, 70 files, +16,023 −3,344 ahead
of `main`), looking for unfinished work, defects, and doc/comment drift. Nothing
here has been fixed yet; each item is written so it can be picked up cold.

> **Worked through 2026-09-01.** Every item now carries a resolution line. §1.1,
> §1.2, §1.3, §1.5, §2.1, §2.3, §2.4, §2.5, §2.6 and §2.7 are **fixed**; §1.4 and
> §2.2 are **won't-do** with a reason; §3a and §3b are **decided**. §4 is
> untouched by design. The verification list at the bottom is corrected where the
> review got it wrong.

## Context

The branch restructures the management API into version namespaces:
version-neutral implementations in `singlestoredb/management/*.py`, backward
overrides in `management/v1/`, pure re-exports in `management/v2/`. It adds the
v2 `Cluster`/`Project` surface plus the Fusion `CLUSTER` grammar, adds
`management/timing.py`, and flips `management.version` to `'v2'`.

The engineering is in good shape: `flake8 singlestoredb/` is clean, ADR 0001's
independence rules are machine-enforced, and the v1/v2 split is expressed as
class attributes rather than `if version ==` branches throughout. The bulk of
what the review found is **documentation and comment drift** — the plan docs were
written before the code landed and were not fully re-read afterwards — plus five
small code issues. Nothing here blocks the branch; the leak risk in §1.1 and the
missing whatsnew entries in §2.2 are the two items worth insisting on.

---

## 1. Code issues

### 1.1 `_is_mocked()` has the wrong fail-safe bias — possible billable leak

`singlestoredb/tests/utils.py` — `_is_mocked(obj)` returns `True` (⇒ **not**
tracked, **not** swept) when `getattr(obj, '_manager', None) is None`. Its
sibling `_creator_is_mocked` documents the opposite policy, and gives the reason:

> An unrecognisable receiver counts as real: a fake deployment swept is a round
> trip and a warning, whereas a real one skipped is a cluster left running and
> billing.

Any real deployment object reaching `_is_mocked` without a populated `_manager`
is silently dropped from tracking. Invert the default so an unrecognisable
object counts as real.

*Verify:* the tracking unit tests in `test_management_utils.py`, plus a new case
asserting an object with `_manager = None` **is** tracked.

**Fixed** in `d9ded284`, as a deletion rather than an inversion: dropping the
`manager is None` branch falls through to `_creator_is_mocked`, which already
answers "real" for `None`. New case
`test_a_deployment_without_a_manager_is_still_tracked`.

### 1.2 Class-fixture timings are inflated

`singlestoredb/tests/conftest.py` — `trace_management_api` appends to
`_management_traces` only `if trace.events:`, but `trace_management_api_class`
computes fixture cost as `trace.elapsed - sum(x.elapsed for x in tests)` over
exactly that list. A test recording zero management events is never subtracted,
so its wall clock is attributed to `setUpClass`. Either append unconditionally,
or subtract a per-class total that counts every test.

*Verify:* `SINGLESTOREDB_MANAGEMENT_TRACE=1 pytest -n 0` on a class mixing
management and non-management tests; reported fixture time should no longer
exceed the real `setUpClass` cost.

**Fixed** in `bea8c065`: append unconditionally, and filter event-less traces at
report time through a new `_traced()` helper — the combined total and both
"slowest" listings all use it, since an empty trace would otherwise inflate
`elapsed` and `unaccounted`. **The runtime check was not run**: it needs the
management suites, which provision real clusters.

### 1.3 mypy error hidden from pre-commit

`singlestoredb/tests/conftest.py:171` — `error: "Item" has no attribute "module"`
under a full `mypy singlestoredb/`. pre-commit's `mirrors-mypy` runs with only
`types-requests`, so `pytest.Item` degrades to `Any` and the error is invisible
there. Fix at the call site (`getattr(item, 'module', None)` or a `cast`).

**Fixed** in `bea8c065` by hoisting the `getattr` to a local. `mypy
singlestoredb/`: 112 errors → 111.

### 1.4 `TTLProperty.reset()` signature break with no callers

`singlestoredb/management/utils.py` — `reset()` became `reset(obj)` as part of the
per-instance caching rework, and has **zero callers in the library**. Keeping it
is right (it is the only way to invalidate the new per-instance cache), but if it
was ever public the break needs a whatsnew line.

**Won't-do.** `reset(obj)` stays as-is — no code change was ever in question. The
whatsnew line is not written, for the reason in §2.2: `whatsnew.rst` is generated
at release time. The break is instead recorded as item 5 of the list the untwist
plan's Part 7 now carries for the release commit messages.

### 1.5 Hardcoded env-var literals in Fusion utils

`singlestoredb/fusion/handlers/utils.py` hardcodes `'SINGLESTOREDB_WORKSPACE'`
(~197, 276, 509, 512) and `'SINGLESTOREDB_PROJECT'` (~371) where
`management/utils.py` already exposes `get_workspace_id()` / `get_cluster_id()`.
Both plan docs describe named constants — `CLUSTER_ENV_VARS`,
`CLUSTER_GROUP_ENV_VAR`, `PROJECT_ENV_VAR` — that **do not exist anywhere in the
codebase**. Reuse the existing accessors and delete those constant names from the
plan docs; introducing the constants is more new surface for no gain.

**Fixed** in `616612eb`, with two amendments to the above:

* The review missed two more of the same reads, in `v2/cluster.py` (`get_cluster`
  and `_resolve_project_id`). Both routed through the accessors too, which let
  `import os` go from that module entirely.
* `SINGLESTOREDB_PROJECT` had **no** accessor, so one was added:
  `get_project_id()`, mirroring `get_workspace_id()`. That is a judgment call
  against the letter of this item — but the argument here is against a *set of
  constants*, and one accessor beside the three already in
  `management/utils.py` is less surface than a literal read left in a different
  package.

`CLUSTER_ENV_VARS` did once exist (`v2/cluster.py`, deleted in `3a9ebb04` when
one variable was left); the plan-doc references to it are now annotated as
historical rather than deleted. `SINGLESTOREDB_WORKSPACE_GROUP` is untouched: the
one site that checks it never reads its value.

---

## 2. Documentation and comment drift

### 2.1 `docs/src/api.rst` has no v2 surface at all — the biggest gap

The branch's only api.rst change is deleting one line
(`Organization.inference_apis`). It still documents **only** v1:
`manage_workspaces`, `WorkspaceManager` and its 12 members, `WorkspaceGroup`,
`Workspace`, `Region` via `WorkspaceManager.regions`, Stage via
`WorkspaceGroup.stage`. Nothing for `manage_clusters`, `ClusterManager`,
`Cluster`, `StarterCluster`, `Project`, or `management.timing`. Since
`management.version` now defaults to `v2`, the published docs describe the
*non-default* API.

The untwist plan already lists this as **outstanding** (`api.rst:233-247`). Add a
cluster section mirroring the existing workspace section's structure, and mark
the workspace section as v1/legacy.

**Fixed** in `8796b5bd`. A cluster section covering `manage_clusters`,
`ClusterManager`, `Cluster`, `StarterCluster` and `Project` now precedes the
workspace one, which is retitled "Workspaces (v1)" with a deprecation note. The
version-neutral sections that reached their manager through `WorkspaceManager` —
Region, Organization, Stage Files — name the `ClusterManager`/`Cluster` attribute
first and the v1 one second.

`management.timing` is **deliberately not documented**: it is internal.

Every autosummary entry was checked to resolve against the source. **Not
Sphinx-built** — see the correction to step 7 below.

### 2.2 No whatsnew entries for the user-visible breaks

`docs/src/whatsnew.rst` needs:

* `manage_cluster` (singular, legacy self-managed clusters) **removed** from
  `singlestoredb/__init__.py`'s exports — zero remaining references in the repo.
* `management.version` now defaults to `'v2'`: a bare `manage_workspaces()` emits
  a deprecation warning, and `manage_clusters()` raises `ManagementError` if the
  option is pinned to `v1`.
* `Portal.cluster_id` now returns `self.workspace_id` rather than reading
  `_connection_info['cluster']` / `SINGLESTOREDB_CLUSTER`; new
  `Portal.project_id`.
* `TTLProperty.reset()` → `reset(obj)`, if §1.4 keeps it.

**Won't-do.** This item conflicts with a decision the branch had already
recorded and the review did not pick up: `docs/src/whatsnew.rst` is generated at
release time by `/bump-version` from the git log, so there is no hand-written
entry to add to. Rather than pre-empt the release, all four breaks (plus
`manage_files`/`manage_regions` resolving to `/v2/`) are now enumerated in the
untwist plan's Part 7 as the list the release commit messages have to carry.
`whatsnew.rst` is untouched on this branch.

### 2.3 ADR 0001 cites two things that don't exist

`docs/adr/0001-versioned-management-api-wrappers.md`:

* ~line 64 lists `JobsManager._legacy_cluster_target_type` — zero hits. The real
  overrides are `_deployment_target_type` and `_starter_target_type`
  (`management/v1/job.py`).
* the inheritance-model block ends "and `v2/stage.py` is a plain re-export" —
  `management/v2/stage.py` does not exist; v2's `Stage` is re-exported from
  `v2/cluster.py`.

The ADR is otherwise accurate: its central claim that
`_version_import._resolve_version()` is the only read of `management.version`
was verified by grep.

**Fixed** in `9206bb09`. Both corrections confirmed against the source first:
`v1/job.py:34-35` and `management/job.py:716-719` hold only the two target-type
attributes, and `v2`'s `Stage` comes from `management/stage.py` via
`v2/cluster.py:40-41`.

### 2.4 Version-neutral modules still say "workspace"

* `management/manager.py:425` — `_wait_on_endpoint`'s docstring says "Workspace
  object with a connect method". Should be deployment-neutral.
* `management/files.py:42` — `FilesObject`'s docstring points at
  ``WorkspaceGroup.stage``; at v2 that is ``Cluster.stage``.

**Fixed:** `manager.py` in `9206bb09`, `files.py` in `8796b5bd` (alongside the
same cross-reference in api.rst's Stage Files section). Both name the v2
attribute first and the v1 one second, rather than replacing one with the other —
the modules are version-neutral and serve both.

### 2.5 Stale `.flake8` per-file-ignore

`.flake8` ignores `singlestoredb/management/inference_api.py`, which moved to
`v1/inference_api.py`. Harmless (flake8 is clean) but misleading.

**Fixed** in `9206bb09` by deleting the line: `v1/inference_api.py` is already
covered by the `singlestoredb/management/v1/*.py:F401` glob two lines down. The
other four `management/*.py` paths in that list were checked and all still exist.

### 2.6 Plan docs left in a pre-landing voice

These read as open questions, but the work landed and the answers are recorded in
`docs/management-api-audit.md`:

* `docs/wait-until-usable-plan.md` — all six steps landed, **not annotated at
  all**. Step 6 still says "Confirm this before implementing", and the snippet it
  proposes differs from what shipped
  (`_resolve_version(version, default=DEFAULT_CLUSTER_VERSION)`).
* `docs/fusion-v2-cluster-plan.md` — Step 4 still reads "probe the password
  behaviour, then decide `WITH PASSWORD`"; the probe ran, results at audit lines
  510-527 / 675 / 699 (`PATCH /v2/clusters/{id}` does not honour
  `adminPassword`). The Risks section still says of `DROP CLUSTER FORCE`
  "Confirm during step 4's probe, and drop the clause if it is a no-op" — the
  clause **was** dropped (`fusion/handlers/cluster.py:685-688` explains why).
  Verification says "expect 45 → ~56 commands"; the registry holds **48**
  (verified: all 11 cluster commands present, zero MODEL handlers).
* `docs/shared-deployment-pool-plan.md` — well annotated; one nit, "Two things
  parallelism does not fix, and one it breaks:" is followed by four bullets.

**Fixed** in `c91db3e2`. Two notes on what the review got slightly wrong:

* The pool plan's four bullets are one not-fixed and **three** breaks (peak
  concurrency, `USE_DATA_API`, the trace summary), not two and two.
* The Fusion plan's "expect 45 → ~56" was **exact at the time** — verified 45 on
  `main` and 56 at the commit the cluster commands landed. The registry holds 48
  only because `0b0765f5` later hid the eight inference and MODEL commands. The
  figure was corrected, but the estimate was not wrong.

### 2.7 Scratch prompt checked into `docs/`

`docs/shared-deployment-pool-prompt.md` was a personal instruction file to an
agent ("Do the plan's steps 1-3. Stop before step 4 … that is mine to run, not
yours."). It is not documentation.

**Fixed** in `3bc9b400` — deleted. The plan it drove is checked in and annotated.

---

## 3. Open decisions — both settled

**a. `management/export.py` is still a 6-line re-export from `.v1.export`.** The
untwist plan (§5 Part 7) says it "repoints to `v2/export.py` **only after** Fusion
cluster support lands". That has landed, so the pin is now either an intentional
deferral or an oversight. Repoint it, or annotate the plan with why it stays.

**Decided: it stays, and the plan is annotated (`c91db3e2`).** Neither deferral
nor oversight exactly — the plan's gate was simply the wrong one. What blocks the
repoint is not `CLUSTER` commands existing, it is the **EXPORT** grammar:
`fusion/handlers/export.py` resolves its target with `get_workspace_group({})` at
every call site, while `v2/export.py`'s `ExportService.__init__` and
`_get_exports` both take a `Cluster`. Repointing the shim breaks every EXPORT
handler with nothing to move them to. The real precondition is porting the EXPORT
Fusion grammar to clusters, which is not on this branch. **Open.**

**b. Does `docs/shared-deployment-pool-prompt.md` stay in the repo?** See §2.7.

**Decided: no.** Deleted in `3bc9b400`.

---

## 4. Unverified risks the branch knowingly carries

Each is already flagged in the branch's own docs; none is actionable here.

* `Portal.cluster_id` / `USE CLUSTER` cannot be exercised outside a Helios
  notebook.
* Nothing bounds concurrent provisioning under `-n`. `utils.deployment_slot()` (a
  `flock` cap) was tried and removed; the ceiling is the org's cluster quota and
  whatever the API tolerates. Marked **Open** in the pool plan.
* `USE_DATA_API=1` with `-n` is untested — `load_sql` ends in `RESTART PROXY`,
  which every worker runs.
* `SINGLESTOREDB_MANAGEMENT_TRACE`'s terminal summary requires `-n 0` (the traces
  live in worker-side module globals).

---

## 5. Scope of the review

Read in full: every plan/ADR doc, `_version_import.py`, `timing.py`, the
`management/utils.py` diff, `management/cluster.py`, all `v1/` and `v2/` override
modules, `fusion/handlers/utils.py`, `tests/utils.py`, the `conftest.py` diff,
and all config/CI/lint diffs. Spot-read: `v2/cluster.py` (1567 lines),
`fusion/handlers/cluster.py` (1027 lines), `management-api-audit.md`.

Not covered: full diffs of `management/stage.py`, `files.py`, `manager.py`,
`organization.py`, `region.py`, `job.py`, `billing*.py` (grepped for terminology
drift only, which produced §2.4); `v1/workspace.py`; and the four large test
diffs (`test_management_v2.py`, `test_management_utils.py`,
`test_management_timing.py`, `test_fusion.py`). No tests were run — starting the
Docker container is a state change.

---

## Verification for the follow-up work

1. `pre-commit run --all-files` → clean. **Ran, clean** — before every commit.
2. `mypy singlestoredb/` → the `conftest.py:171` error gone; total drops by
   exactly one (the rest is pre-existing third-party/numpy noise). **Ran: 112 →
   111**, exactly as predicted.
3. `pytest -v -m 'not management' singlestoredb/tests` → green, no token needed.
   **Ran: 762 passed, 15 skipped.**
4. `python -c "import singlestoredb.fusion, singlestoredb.fusion.registry as r; print(len(r._handlers))"`
   → 48, if the Fusion doc figures are corrected. **Ran: 48**, all 11 cluster
   commands present, zero MODEL handlers.
5. `pytest -v -m 'management and not management_v1' singlestoredb/tests` → green
   (what the `-n 3 --dist loadgroup` default is tuned for). **Not run** — it
   provisions real billable clusters.
6. Nightly gate unaffected: `pytest -v -m 'management_v1' singlestoredb/tests`.
   **Not run**, same reason.
7. Docs build after the api.rst/whatsnew work: `make -C docs html`, no new Sphinx
   warnings.

   **Not run, and the command is wrong.** There is no `docs/Makefile` — it is
   `docs/src/Makefile`, so the invocation is `make -C docs/src html`. Worth
   knowing before running it: that Makefile's catch-all target starts a
   SingleStoreDB Docker container (the `ipython_directive` extension executes
   code) and then `mv`s the build output over the ~203 committed HTML files in
   `docs/`, so an innocent-looking docs check produces a large unrelated diff.
   `sphinx-build -b html . _build/check` from `docs/src` is the side-effect-free
   way to look for warnings.

   The api.rst work in §2.1 therefore ships **unbuilt**. It was checked by
   resolving every autosummary entry against the source and verifying every
   section underline, which is not the same as a clean Sphinx run.

Also unverified: §1.2's runtime check
(`SINGLESTOREDB_MANAGEMENT_TRACE=1 pytest -n 0` on a mixed class) needs the
management suites, so the fix is argued from the code, not measured.
