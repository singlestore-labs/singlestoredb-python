# Implementation prompt — Stage 2 (+1c) of the round-trip plan

Read `docs/stage-upload-round-trips-plan.md` first. Implement **Stage 2** and
**Stage 1c**. Do not touch Stage 3 or Stage 4.

Constraints from the plan that are already decided — do not reopen them:

* **No caching of any kind.** No name→ID memo, no statement-scoped cache. Both
  were evaluated and rejected in the plan (the cross-statement one for
  staleness, the statement-scoped one because it has a 0% hit rate — nothing is
  fetched twice inside one statement). If you think you have found a case that
  needs one, say so and stop rather than adding it.
* The existing one-hour `ttl_property` on `ClusterManager.projects`
  (`singlestoredb/management/v2/cluster.py:1015`) **stays**. It is what keeps
  `SHOW CLUSTERS EXTENDED` at one `GET /v2/projects` for N rows once `.project`
  is lazy.

## Work items

**Stage 2 — make `Cluster.project` lazy.** Remove the eager
`_project_from_id(manager, obj.get('projectID'))` from `Cluster.from_dict`
(`v2/cluster.py:409`) and `StarterCluster.from_dict` (`v2/cluster.py:813`); keep
the `projectID` on the instance and resolve `Project` on first access to
`.project`. Preserve the current behaviour exactly: `None` project ID yields
`None`, and an ID matching no project yields `Project(id=<id>, name='<unknown>')`
so `cluster.project.id` is always readable (see `_project_from_id`'s docstring at
`v2/cluster.py:55`). The two readers that must keep working are
`fusion/handlers/cluster.py:74` and `v2/cluster.py:1207`. Watch for anything that
assigns `self.project` (`v2/cluster.py:269,775`) or constructs these classes
outside `from_dict`.

Leave `get_deployment` (`fusion/handlers/utils.py:480-495`) alone otherwise. An
earlier draft also swapped its client-side name filter for
`GET /v2/clusters?name=<name>`; that is **dropped, because the API has no name
filter.** Only ID lookup is server-side (`ClusterManager.get_cluster(id)` →
`GET /v2/clusters/<id>`, already used by `_deployment_by_id`), so resolving a name
means listing every cluster and filtering client-side. Do not try to add a name
query param.

**Stage 1c — collapse the duplicate metadata GET in the `OVERWRITE` path.** In
`Stage._upload` (`singlestoredb/management/stage.py:287-291`), `exists()` is
`info()` behind a `try` and `remove()` opens with `is_dir()` — the same `GET` on
the same path twice. Fetch the metadata once and branch on the object: absent →
`PUT`; present and not `overwrite` → `OSError` with today's message; present and
a directory → `IsADirectoryError` with today's message; otherwise `DELETE` then
`PUT`. Do the same in `FileSpace._upload` (`management/files.py:803-806`). Leave
`remove()` itself alone — its `is_dir()` is correct for its other callers.

## Verification

Goal-driven, in this order:

1. **Build the request-count harness first**, before any of the changes above.
   The plan calls for it and it does not exist yet; it is what makes 2 and 3
   checkable, so write it reusable rather than inlined into one test. It should
   count requests through a mocked manager. Verify: it reproduces the *current*
   counts on unmodified code — four for a plain `UPLOAD FILE TO STAGE ... IN
   '<name>'`, six with `OVERWRITE`.
2. Then implement, and pin the new counts: three plain, four with `OVERWRITE`,
   and assert **no** `GET /v2/projects` is issued by an upload.
3. Assert `SHOW CLUSTERS EXTENDED` still reports the project name and issues
   `GET /v2/projects` exactly once regardless of cluster count.
4. Existing coverage must still pass unchanged: `test_upload_file` in
   `singlestoredb/tests/test_management_v2.py:1500` and
   `test_management_v1.py:380` (conflict `OSError` and the `overwrite=True`
   path).
5. `pytest -v -m 'management and not management_v1' singlestoredb/tests` for the
   live management suite; see `CLAUDE.md` for the `-n 3 --dist loadgroup`
   defaults and why not to override them.

Run `pre-commit run --all-files` and fix everything it flags before committing.
Update `docs/stage-upload-round-trips-plan.md` to mark what landed.
