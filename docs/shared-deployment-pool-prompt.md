Implement the shared deployment pool described in
`docs/shared-deployment-pool-plan.md`. Read that file first; it has the measured
numbers, the audit that established which suites are safe, and the one real
hazard. Work on branch `versioned-management-api`.

Context you need that isn't in the repo:

- The goal is cutting *serial* wall time on the management suite by deploying
  fewer clusters. A traced run spends 8874s of 8915s inside the management API,
  so nothing local matters. An S-00 cluster takes ~460s to reach ACTIVE and that
  is irreducible.
- Four classes (`TestStageFusion`, `TestJobsFusion`,
  `test_management_v2.py::TestStage`, `test_management_v2.py::TestJob`) deploy
  5 clusters between them for 2190s of fixture time, and all four need only a
  live cluster. Two shared clusters cover all four, for ~890s. That ~1300s is
  the whole deliverable.
- The hazard, which will silently break a naive implementation: the pool must be
  tracked under the *empty* owner. `conftest.py::pytest_runtest_setup` sweeps the
  previous class's tracked deployments when the run moves to the next class, so a
  cluster created inside a `setUpClass` is terminated after its first consumer.
  The plan has the exact `utils.set_owner('')` block to use. No `conftest.py`
  change is needed.

Do the plan's steps 1-3. Stop before step 4 and the optional follow-ups: step 4
needs a real traced run against a live org, which needs
`SINGLESTOREDB_MANAGEMENT_TOKEN` and takes tens of minutes, so that is mine to
run, not yours.

Constraints:

- Do not change what any test asserts. This is a fixture change only. If a test
  looks like it needs rewriting to share a cluster, stop and tell me instead --
  that means the audit missed something.
- Do not pool `test_management_v2.py::TestCluster`,
  `test_management_v1.py::TestWorkspace`, `TestWorkspaceFusion`,
  `TestClusterFusionCreateDrop` or `TestClusterFusionSuspendResume`. The plan
  says why for each.
- Preserve the existing skip behaviour exactly (no US regions / no STANDARD
  project must still skip, not error).
- Run `pre-commit run --files <changed files>` and fix what it flags before
  committing. Repeat until clean.
- `pytest -m 'not management' singlestoredb/tests/test_fusion.py
  singlestoredb/tests/test_management_v1.py
  singlestoredb/tests/test_management_v2.py` must still pass (88 tests as of
  this writing). It starts a Docker container automatically and takes ~10s.
  Be explicit in your report that the management-marked tests are NOT covered by
  this -- they need a token and I have to run them.

Also note, so you don't re-derive it: `test_fusion.py`'s `TestClusterFusion` was
recently split into five classes behind a `_ClusterFusionMixin`, which declares
`fixture_prefixes` to say how many clusters each class needs. That is the closest
existing pattern to what you are building, and it is a reasonable model for how
a class should declare its pool needs. It is unrelated to the pool work
otherwise. There is one open question on it I have not answered -- whether
`TestClusterFusionSuspendResume` keeps its own cluster or goes back to sharing
`TestClusterFusion`'s three -- so leave that class alone.
