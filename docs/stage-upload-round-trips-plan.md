# Cutting the round trips in the Stage and Files write paths

`UPLOAD FILE TO STAGE 'stats.csv' IN '<name>' FROM 'mydata.csv'` cost **six HTTP
requests** to upload one file when this was written. Only one of them transfers
anything. This plan is staged so each part lands and ships on its own. Stages 1
and 2 have landed and took the count to **three**, or four with `OVERWRITE`.
Stage 3 and Stage 4 are open.

## Measured baseline

Against a live organization (`S2DB Eng - Launchpad`, one ACTIVE cluster), for a
60-byte CSV:

| # | Call | Origin | Typical |
|---|------|--------|---------|
| 1 | `GET /v2/clusters` | `get_deployment` resolving `IN '<name>'` | ~2000-3600 ms |
| 2 | `GET /v2/projects` | `Cluster.from_dict` → `_project_from_id`, per manager | ~280 ms |
| 3 | `GET .../stage/fs/<path>?metadata=1` | `Stage.upload_file`'s `exists()` | ~1000 ms |
| 4 | `GET .../stage/fs/<path>?metadata=1` | `Stage._upload`'s `exists()` — **the same check** | ~1000 ms |
| 5 | `PUT .../stage/fs/<path>` | the upload | 1500-31000 ms |
| 6 | `GET .../stage/fs/<path>?metadata=1` | `_upload` returns `info()`, which Fusion **discards** | ~1000 ms |

With `OVERWRITE` against a file that already exists, `remove()` inserts an
`is_dir()` metadata `GET` plus a `DELETE`, making it eight.

Rows 4 and 6 are gone as of `400d5b71` — Stage 1. Row 2 is gone with Stage 2,
and Stage 1c collapsed the `OVERWRITE` path's second metadata `GET`. The live
count is now rows 1, 3 and 5: **three**, or four with `OVERWRITE`.

One call the table missed: a cluster payload that carries a `region` makes
`Cluster.from_dict` resolve it against `ClusterManager.regions`, which is a
`GET /v2/regions` on the first cluster a manager builds — the same eager-resolve
shape Stage 2 removed from the project, and the same one-hour `ttl_property`
holding it to one call per manager. So the live figure for a fresh manager is
one more than the count above. It is pinned by
`test_a_region_on_the_payload_costs_a_region_request` rather than fixed;
`Cluster.region` would want the same lazy treatment as `Cluster.project`, and
that is not in this plan.

### The part we cannot fix

Stage route latency is erratic and payload-independent. Repeated 8-byte uploads
to the same cluster measured `PUT` at 30665 ms, 1534 ms and 2522 ms; a `DELETE`
of an 8-byte file took 27214 ms while another took 1376 ms. A 100 KB `PUT` took
31286 ms — the same range as 8 bytes, so this is not transfer time.

Two consequences for this plan. First, no client change makes an upload reliably
fast; the ceiling is the route. Second, **reducing the call count is still the
right work**, because every call is an independent chance to draw a 30-second
stall. Going from six calls to three halves the exposure. Do not expect the
stopwatch to prove it on any single run — the variance swamps the difference.
Stage 4 exists to get the route itself looked at.

### Two facts established while measuring

* There is **no server-side filter by name.** A cluster can be fetched by ID —
  `ClusterManager.get_cluster(id)` (`v2/cluster.py:1455`) is a path lookup,
  `GET /v2/clusters/<id>` — but a name can only be resolved by listing every
  cluster and filtering client-side. This is why `IN '<id>'` already costs one
  call, through `_deployment_by_id` (`fusion/handlers/utils.py:566-575`), and only
  `IN '<name>'` pays the full listing. Nothing in this plan changes that; the
  listing is the floor for the name spelling.
* The listing cost is fixed route overhead, not payload: ~2245 ms for a
  one-cluster org. Trimming what comes back would not help even if it were
  possible.
* The `PUT` response body is only `{"name": ..., "path": ...}`. It carries none
  of `size`, `type`, `format`, `mimetype` or `writable`, so
  `FilesObject.from_dict` cannot be fed from it. `_upload` cannot skip its
  trailing `info()` by reusing the write response, which is why Stage 1b has to
  be done at the caller instead.

## Stage 1 — remove the two redundant calls in the upload path — **landed (`400d5b71`)**

Independent of every other stage. Two unambiguous defects, no design question.

### 1a. The duplicated `exists()` / `remove()`

`Stage.upload_file` (`singlestoredb/management/stage.py:188-197`) checks
`exists()`, raises or `remove()`s, then delegates to `_upload`, which does
exactly the same thing again (`stage.py:293-296`). `FileSpace.upload_file` and
`FileSpace._upload` carry the identical pair (`management/files.py:694-704` and
`files.py:803-806`).

Delete the check from both `upload_file` methods and let `_upload` own it. The
messages are already identical within each class — `'stage path already
exists'`, `'file path already exists'` — so nothing observable changes.

One wrinkle to handle rather than inherit: `upload_file` currently opens the
local file *after* its `exists()` check, so removing the check means the handle
is opened before `_upload` raises `OSError` on a non-`overwrite` conflict,
leaking it until GC. Wrap the `open()` in a `with` block.

`upload_folder` calls `upload_file` per file (`stage.py:267`), so a folder upload
saves one call per file.

**Verify:** existing `test_upload_file` coverage in `test_management_v2.py:1500`
and `test_management_v1.py:380` already asserts the conflict `OSError` and the
`overwrite=True` path; both must still pass. Add a unit test that counts
requests through a mocked manager and pins the count, so the redundancy cannot
come back — that harness is the one thing this plan needs that does not exist
yet.

### 1b. The discarded `info()`

`_upload` ends `return self.info(stage_path)`, and
`UploadStageFileHandler.run` (`fusion/handlers/stage.py:199-204`) throws the
result away. Same in `fusion/handlers/files.py:201` and
`fusion/handlers/models.py:154` — all three Fusion upload handlers return
`None`.

The `PUT` body cannot supply the `FilesObject` (see above), and `upload_file`'s
public contract returns one, so the `info()` cannot simply go. Give the handlers
a path that does not ask for it. Preferred shape: a private
`_upload(..., fetch_info: bool = True)` returning `Optional[FilesObject]`, with
the three Fusion handlers calling `_upload(..., fetch_info=False)` through a thin
`upload_file`-shaped helper so they keep the `IsADirectoryError` check on the
local path.

**Verify:** the request-count test from 1a covers this too. Assert that the
public `upload_file` still returns a populated `FilesObject`.

**Stage 1 payoff:** six calls to four, or eight to six with `OVERWRITE`.

### 1c. The `OVERWRITE` path fetches the same metadata twice — **landed**

Left over from 1a rather than introduced by it. `_upload` (`stage.py:287-291`)
calls `exists()`, which is `info()` behind a `try` (`stage.py:395`), and then
`remove()`, which opens with `is_dir()` — `info()` again (`stage.py:732`), on the
same path, with nothing in between that could have changed it.

Fetch the metadata once in `_upload` and branch on the object: absent → `PUT`;
present and not `overwrite` → `OSError`; present and a directory →
`IsADirectoryError`; otherwise `DELETE` and `PUT`. No caching, no new state —
the second call is reading a value the frame already holds. `remove()` keeps its
own `is_dir()` for its other callers.

`FileSpace._upload` (`files.py:803-806`) carries the same pair.

**Verify:** the request-count harness pins the `OVERWRITE` count at four. The
`IsADirectoryError` that `remove()` currently raises through `_upload` must
still be raised, with the same message.

**1c payoff:** six calls to five with `OVERWRITE`; nothing on the plain path.

**Landed as:** the shared `FileLocation._info_or_none` (`management/files.py`),
which `Stage._upload` and `FileSpace._upload` branch on. `remove()` keeps its
own `is_dir()`, as planned. Pinned by
`test_an_overwrite_costs_one_check_and_one_delete`,
`test_an_overwrite_of_a_folder_raises_on_the_one_check` and the two file-space
equivalents in `test_management_utils.py`.

## Stage 2 — resolve the deployment in one call, not two — **landed**

Depends on nothing in Stage 1. No caching, no new state, no open decision.

`get_deployment` resolves `IN '<name>'` by filtering `manager.clusters`
(`fusion/handlers/utils.py:491`), which costs `GET /v2/clusters`. It then costs a
second call it never uses: `Cluster.from_dict` resolves `_project_from_id` for
every cluster in the listing (`v2/cluster.py:409` and `:813`), and that reads
`ClusterManager.projects`, so name resolution drags `GET /v2/projects` (~280 ms)
along behind it.

### Why there is no caching here

An earlier draft of this stage proposed memoizing name → ID, on the theory that a
notebook looping four `CREATE STAGE FOLDER ... IN '<name>'` statements pays name
resolution four times. It does, but that is **cross-statement** state, and the
staleness it buys is not worth it: a renamed or replaced cluster keeps resolving
to the old ID until the entry expires, and `DROP CLUSTER` / `CREATE CLUSTER`
would each need to invalidate it. Dropped.

A **statement-scoped** cache — the narrow, obviously-safe version — was checked
and is dead weight. Inside one statement there is nothing to hit twice:

* every stage handler calls `get_deployment` exactly **once** per `run`
  (`fusion/handlers/stage.py:92,199,293,370,435,499`);
* `ClusterManager.projects` is *already* a one-hour `ttl_property`
  (`v2/cluster.py:1015`), so `_project_from_id` costs one `GET /v2/projects` per
  manager no matter how many clusters the listing holds.

A per-statement memo would have a 0% hit rate on the upload path. The fix is not
to cache the second call, it is to not make it.

### The change

* **Make `Cluster.project` lazy.** This is the whole payoff. Stop calling
  `_project_from_id` in `Cluster.from_dict`; keep the `projectID` and resolve
  `Project` on first access to `.project`. There are exactly two readers —
  `fusion/handlers/cluster.py:74` (`SHOW CLUSTERS EXTENDED`) and
  `v2/cluster.py:1207` — and both must keep working, including the
  `Project(id=..., name='<unknown>')` fallback for an ID that matches no project.
  `StarterCluster` (`v2/cluster.py:813`) gets the same treatment. Note that the
  `ttl_property` on `projects` stays useful: `SHOW CLUSTERS EXTENDED` reads
  `.project` per row, and one manager must still serve all of them from one
  fetch.

An earlier draft paired this with server-side filtering
(`GET /v2/clusters?name=<name>`) in place of the list-everything-then-filter in
`get_deployment`. Dropped: **the API has no name filter.** Only ID lookup is
server-side, and that path is already taken by `_deployment_by_id`. The
client-side filter and its ambiguity check stay exactly as they are.

### What is left afterwards

One call, `GET /v2/clusters`, at a ~2000 ms floor that is fixed route overhead.
That floor is then the entire cost of name resolution and there is nothing
further the client can do about it — it is Stage 4's reporting job.

**Verify:** the request-count harness from Stage 1, extended to count a whole
Fusion statement rather than a `Stage` call. Pin the count for
`UPLOAD FILE TO STAGE ... IN '<name>'` at three, and assert no
`GET /v2/projects` is issued. Separately assert `SHOW CLUSTERS EXTENDED` still
reports the project name and issues `GET /v2/projects` exactly once regardless of
cluster count.

**Landed as:** `Cluster.project` and `StarterCluster.project` are properties
over a stored `_project_id`, resolved by `_lazy_project` on first read
(`v2/cluster.py`). `_project_from_id` and its `<unknown>` fallback are unchanged
and still what does the resolving; a `Project` passed to the constructor is
still kept as it stands. Two consequences worth knowing:

* `str(cluster)` no longer includes `project=...`. `vars_to_str` skips
  underscored attributes, and the alternative — resolving in `__repr__` — would
  make printing a cluster issue a request.
* `SHOW CLUSTERS EXTENDED` reports `ProjectID`, not the project name; the plan
  said "name". `TestStatementRoundTrips.test_show_clusters_extended_reports_the_project_once`
  asserts the column the handler actually has and reads `.project.name` off the
  clusters to cover the name.

**The harness:** `CountingClusterManager`, `counting_cluster_manager` and
`run_fusion_statement` in `singlestoredb/tests/utils.py`, next to the
`CountingManager` Stage 1 introduced. It serves the management routes a
statement resolves a deployment through from fixture payloads, delegates the
Stage filesystem routes to a `CountingManager` sharing its `calls` list, and
raises on any route nobody accounted for. Before Stages 2 and 1c landed it
reproduced the counts in the table above exactly: four for a plain upload, six
with `OVERWRITE`. Stage 3 should extend it rather than write another one.

## Stage 3 — the folder and listing paths

Depends on nothing. Lower priority; same class of defect, different methods.

* `mkdir` (`stage.py:322-334`, `files.py:815+`) does `exists()`, then possibly
  `info()`, then `PUT`, then `info()` again — up to four calls to create one
  folder. `CREATE STAGE FOLDER` discards the return, exactly like 1b. The
  `exists()` and the `info()` are the same `GET` on the same path back to back,
  so the same one-fetch-and-branch rewrite as 1c applies.
* `remove` calls `is_dir()`, which is a full `info()`, before its `DELETE`. 1c
  stops the upload path paying for it; `remove` keeps it for its other callers.
* `SHOW STAGE FILES ... EXTENDED` calls `stage.info(x)` **per entry** on top of
  the `listdir` (`fusion/handlers/stage.py:105-116`). A 20-file listing is 21
  calls. Check whether `listdir` can be asked for metadata in one request; if
  not, this one is inherent and should be documented as such rather than
  "fixed".

## Stage 4 — get the route latency looked at

Not an SDK change. The numbers in this document — 30-second stalls on 8-byte
writes, a 2-second floor on `GET /v2/clusters` — belong with whoever owns those
routes. Two things worth doing here:

* Extend `SINGLESTOREDB_MANAGEMENT_TRACE` coverage so Stage calls show up in the
  per-route breakdown the way management calls already do, giving anyone
  reporting this a reproduction rather than an anecdote.
* File the `GET /v2/clusters` floor separately from the Stage `PUT`/`DELETE`
  variance. They are different routes and probably different causes.

## Order and independence

Stages 1, 2 and 3 touch disjoint code and can land in any order or in parallel.
Stages 1 and 2 have landed. Stage 3 is what is left of the SDK-side work, and
Stage 4 is reporting and can proceed alongside.

The request-count harness is in `singlestoredb/tests/utils.py`
(`CountingManager` for a `Stage` or `FileSpace` call, `CountingClusterManager`
plus `run_fusion_statement` for a whole statement) and is what makes Stage 3
checkable.
