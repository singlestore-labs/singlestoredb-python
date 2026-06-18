# Management API ↔ OpenAPI gap audit

Comparison of `singlestoredb/management/v1/*` Python wrappers against the
endpoint and schema definitions in `management-openapi.txt`. Scope is field
coverage and parameter coverage **only** — no new endpoint classes, no new
methods. Hand-rolled file abstractions, the `fields=` query param, and DR /
identity / privateConnections / delegatedEntities sub-resources are out of
scope per the task scoping.

For each class:
- **Missing fields** = present in OpenAPI schema, absent from the Python class.
- **Mistyped / under-parsed fields** = present in both, but parsing in
  `from_dict` should be tightened (e.g. raw string where the spec marks
  `date-time`).
- **Missing params** = a manager method backs an endpoint that accepts a query
  / body field the Python signature does not expose.
- **Flag-only** = surfaced for awareness but no fix proposed in this pass.

All `__init__` changes append trailing `Optional[...] = None` parameters.
Parsing/typing happens in `from_dict`; `__init__` widens to accept raw input.

---

## 1. `Workspace` ↔ `Workspace` / `WorkspaceCreate` / `WorkspaceUpdate`

File: `singlestoredb/management/v1/workspace.py:712`

### Missing fields on `Workspace`

| OpenAPI field | Type | Notes |
|---|---|---|
| `autoScale` | `AutoScale` (object) | New nested object: `changedAt`, `lastAutoScaledAt`, `maxScaleFactor`, `sensitivity` |
| `kaiEnabled` | `boolean` | Indicates whether Kai is enabled on this workspace |
| `scaleFactor` | `number` | Returned by API, not currently captured |

**Fix:** add three trailing kwargs to `__init__`:
```python
auto_scale: Optional[Dict[str, Any]] = None,
kai_enabled: Optional[bool] = None,
scale_factor: Optional[float] = None,
```
…and three lines in `from_dict`:
```python
auto_scale=obj.get('autoScale'),
kai_enabled=obj.get('kaiEnabled'),
scale_factor=obj.get('scaleFactor'),
```

### Mistyped / under-parsed fields

| Field | Current | Spec | Proposed `from_dict` fix |
|---|---|---|---|
| `state` | `str` (raw, with `.strip()`) | `enum(ACTIVE\|PENDING\|SUSPENDED\|FAILED\|TERMINATED)` | Keep `str` for compat; flag-only. (No existing enum class to map to.) |
| `deployment_type` | `Optional[str]` | `enum(PRODUCTION\|NON-PRODUCTION)` | Same — flag-only. |
| `cache_config` | `Optional[int]` | `number` (per spec — fractional values legal) | Widen attribute type to `Optional[float]`. Existing callers passing `int` still work. |

### `WorkspaceManager.create_workspace` — missing body fields

`POST /v1/workspaces` accepts `WorkspaceCreate`, which includes:

- `autoScale` — **missing** from method signature & posted body
- `scaleFactor` — **missing**

**Fix:** append to signature:
```python
auto_scale: Optional[Dict[str, Any]] = None,
scale_factor: Optional[float] = None,
```
And add to the posted JSON dict (with `snake_to_camel_dict` for `auto_scale`).
Mirror the same change on `WorkspaceGroup.create_workspace` (the convenience
wrapper that calls through to `WorkspaceManager.create_workspace`).

### `Workspace.update` — missing body fields

`PATCH /v1/workspaces/{id}` accepts `WorkspaceUpdate`. Method currently exposes
`auto_suspend`, `cache_config`, `deployment_type`, `size`. Missing:

- `autoScale`
- `enableKai` (spec: `enum(True|False)`)
- `scaleFactor`

**Fix:** append the three params and add them to the JSON dict in `update()`.

---

## 2. `WorkspaceGroup` ↔ `WorkspaceGroup` / `WorkspaceGroupCreate` / `WorkspaceGroupUpdate`

File: `singlestoredb/management/v1/workspace.py:1054`

### Missing fields on `WorkspaceGroup`

| OpenAPI field | Type | Notes |
|---|---|---|
| `deploymentType` | `enum(PRODUCTION\|NON-PRODUCTION)` | Already on `Workspace`; not on group |
| `expiresAt` | `string` (date-time-ish) | Server-set TTL |
| `highAvailabilityTwoZones` | `boolean` | |
| `optInPreviewFeature` | `boolean` | |
| `outboundAllowList` | `string` | Account ID for outbound connections |
| `projectID` | `string:uuid` | |
| `projectName` | `string` | |
| `smartDRStatus` | `enum(ACTIVE\|STANDBY)` | (Field is `smartDRStatus` on the response — separate from the `smart_dr` POST input) |
| `state` | `enum(ACTIVE\|PENDING\|FAILED\|TERMINATED)` | **Required by spec.** Currently absent — significant gap. |
| `updateWindow` | `UpdateWindow` (object) | `{day: int, hour: int}` |

**Fix:** append all 10 as trailing optional kwargs to `__init__`. Map them in
`from_dict`. For `expires_at` and `terminated_at` use `to_datetime`. For
`update_window`, store the raw dict (callers already pass it as a dict
elsewhere). For `state`, keep it `Optional[str]` for compat with existing call
sites but parse it raw.

### Mistyped / under-parsed fields

| Field | Current | Spec | Proposed `from_dict` fix |
|---|---|---|---|
| `created_at` | `Union[str, datetime.datetime]` → `to_datetime` | `string` (no format declared, but de-facto date-time) | OK as-is. |
| `region` | resolved by lookup against `manager.regions` | spec returns `regionID`, `regionName`, `provider` directly | Keep current behavior (lookup is richer); also store raw `provider` and `regionName` from the response onto the group itself for cases where lookup misses. |
| `allow_all_traffic` | `Optional[bool]` (coerced to `False` if missing) | `bool` | OK. |

### `WorkspaceManager.create_workspace_group` — missing/misnamed body fields

Currently posts: `name`, `regionID`, `adminPassword`, `backupBucketKMSKeyID`,
`dataBucketKMSKeyID`, `firewallRanges`, `expiresAt`, `smartDR`,
`allowAllTraffic`, `updateWindow`.

| OpenAPI field | Status |
|---|---|
| `regionID` | present |
| `regionName` | **missing** — spec accepts either `regionID` or `regionName` |
| `provider` | **missing** |
| `firewallRanges` | present (required) |
| `name` | present (required) |
| `adminPassword` | present |
| `expiresAt` | present |
| `allowAllTraffic` | present |
| `updateWindow` | present |
| `deploymentType` | **missing** |
| `highAvailabilityTwoZones` | **missing** |
| `optInPreviewFeature` | **missing** |
| `projectID` | **missing** |
| `backupBucketKMSKeyID`, `dataBucketKMSKeyID`, `smartDR` | **not in OpenAPI spec.** Flag-only — these may be undocumented endpoints. Don't remove. |

**Fix:** append `provider`, `region_name`, `deployment_type`,
`high_availability_two_zones`, `opt_in_preview_feature`, `project_id` as
trailing kwargs and add them to the posted JSON.

### `WorkspaceGroup.update` — missing body fields

Currently posts: `name`, `firewallRanges`, `adminPassword`, `expiresAt`,
`allowAllTraffic`, `updateWindow`. Missing per `WorkspaceGroupUpdate`:

- `deploymentType`

**Fix:** append `deployment_type: Optional[str] = None` and add to JSON.

### List query params

`GET /v1/workspaceGroups` accepts `includeTerminated`. The
`WorkspaceManager.workspace_groups` property does not pass it.
**Flag-only** — `workspace_groups` is a `@property`, not a method, so adding
a param requires reshaping. Skipping per scope.

`GET /v1/workspaces` accepts `workspaceGroupID` and `includeTerminated`. Only
`workspaceGroupID` is passed (in `WorkspaceGroup.workspaces` property).
`includeTerminated` is unsupported. **Flag-only** — same property/method
shape issue.

---

## 3. `StarterWorkspace` ↔ `SharedTierVirtualWorkspace`

File: `singlestoredb/management/v1/workspace.py:1359`

### Missing fields on `StarterWorkspace`

| OpenAPI field | Type | Notes |
|---|---|---|
| `mysqlDmlPort` | `integer` | |
| `websocketPort` | `integer` | |
| `projectID` | `string:uuid` | |

**Fix:** append three trailing kwargs and map in `from_dict`.

### Mistyped / under-parsed fields

None. All currently-present fields match the spec.

### `WorkspaceManager.create_starter_workspace` — missing body fields

Posts: `name`, `databaseName`, `provider`, `regionName`. Per
`SharedTierCreateVirtualWorkspace`:

- `projectID` — **missing**

**Fix:** append `project_id: Optional[str] = None` and include in payload.

Also: spec marks `provider` as `enum(AWS|GCP|AZURE)`. Method docstring says
`'aws', 'gcp', 'azure'`. Spec is case-sensitive. **Flag-only** — accept either
or upcase before posting.

### `StarterWorkspace.create_user` ↔ `SharedTierCreateUser`

Posts: `userName`, `password`. Spec matches. No gap.

No method exists for `PATCH /v1/sharedtier/virtualWorkspaces/{id}/users/{id}`
(`SharedTierUpdateUser`). **Flag-only** — out of scope (would be a new method).

---

## 4. `Job` + sub-objects ↔ `Job` / `JobCreate` / `JobMetadata` / etc.

File: `singlestoredb/management/v1/job.py`

### `Job` (line 549)

Field coverage is complete. All 11 schema fields are mapped in `from_dict`.

### `JobMetadata` (line 193)

Complete. All 4 fields (`avgDurationInSeconds`, `count`,
`maxDurationInSeconds`, `status`) mapped.

### `Schedule` (line 449) ↔ `JobSchedule`

Complete. `executionIntervalInMinutes`, `mode`, `startAt` all mapped.

### `TargetConfig` (line 497) ↔ `JobTargetConfig`

Complete.

### `ExecutionConfig` (line 401) ↔ `JobExecutionConfig`

**Verified.** `from_dict` already reads `obj['maxAllowedExecutionDurationInMinutes']`
correctly. The Python attribute name `max_duration_in_mins` is just a short
local alias — not a bug. **No fix needed for the read path.**

However, `JobsManager.schedule()` builds the request body with **only**
`createSnapshot` and `notebookPath`. The required spec field
`maxAllowedExecutionDurationInMinutes` is **never sent** on POST.

**Fix:** add `max_allowed_execution_duration_in_minutes: Optional[int] = None`
as a trailing kwarg on `JobsManager.schedule()`, and include it in the
`execution_config` dict when not `None`.

### `Execution` (line 289) ↔ `Execution`

Complete. All 8 fields mapped.

### `ExecutionMetadata` (line 245) ↔ `ExecutionsMetadata`

Spec only declares `startExecutionNumber`, `endExecutionNumber` (both
required). Python `__init__` matches if those are the two fields present. No
gap.

### `Parameter` (line 101) ↔ `JobParameter`

Spec field `type` is `enum(string|integer|float|boolean)`. Python stores
`type: str` — accepts any string. **Flag-only** — accepting any string is
forgiving; fixing to enum is a behavioral narrowing.

### `Runtime` (line 149) ↔ `JobRuntime`

`JobRuntime` is referenced in the spec but its definition is empty in the
text dump (`JobRuntime: None`). Python class has `name`, `description`.
**Flag-only** — likely a tooling artifact in the spec dump; verify.

### `JobsManager.schedule` — body coverage vs `JobCreate`

`JobCreate` body fields: `name`, `description`, `executionConfig`, `schedule`,
`targetConfig`, `parameters`. Method covers `name`, `description`,
`schedule`, `targetConfig`, `parameters`, plus the executionConfig
sub-fields `createSnapshot` and `notebookPath`.

**Verified:** `parameters` is correctly converted to a list of
`{name, value, type}` triples via `type_to_parameter_conversion_map`. No fix.

**Gap:** the `executionConfig` body is missing
`maxAllowedExecutionDurationInMinutes` (covered above in `ExecutionConfig`
section).

### `JobsManager.get_executions` — query param coverage

**Verified.** `singlestoredb/management/v1/job.py:863` builds the URL as
`f'jobs/{job_id}/executions?start={start_execution_number}&end={end_execution_number}'`.
The query keys `start` / `end` already match the spec. **No fix needed.**

---

## 5. `FilesObject` ↔ `FileObjectMetadata`

File: `singlestoredb/management/v1/files.py:34`

Per scoping, only field-coverage of the metadata response is in scope.

### Field coverage

| Spec field | Status |
|---|---|
| `name` | present |
| `path` | present |
| `size` | present (int) |
| `type` | present (`enum(\|json\|directory)` accepted as str) |
| `format` | present (`enum(json)`) |
| `mimetype` | present |
| `created` | present (parsed via `to_datetime`) |
| `last_modified` | present (parsed via `to_datetime`) |
| `writable` | present (bool) |
| `content` | present |

No missing fields; types align.

---

## 6. `Organization` ↔ `Organization`

File: `singlestoredb/management/v1/organization.py:114`

### Field coverage

| Spec field | Python | Status |
|---|---|---|
| `orgID` (required) | `id` | present |
| `name` | `name` (default `'<unknown>'`) | present |
| `firewallRanges` | `firewall_ranges` | present |

No gaps.

---

## 7. `Secret` ↔ `Secret` / `SecretCreate` / `SecretUpdate`

File: `singlestoredb/management/v1/organization.py:29`

### Field coverage

`Secret.__init__` takes `id`, `name`, `created_by`, `created_at`,
`last_updated_by`, `last_updated_at`, `value`, `deleted_by`, `deleted_at`.
That covers every field in the spec.

### Mistyped / under-parsed fields

| Field | Current | Spec | Proposed fix |
|---|---|---|---|
| `created_at` | `Union[str, datetime]` stored raw | `string` (date-time) | Parse via `to_datetime` in `from_dict`. |
| `last_updated_at` | same | same | same |
| `deleted_at` | same | same | same |

The class stores these raw rather than parsing — fix in `from_dict`.

### Manager-side gaps

There is no `SecretManager` class — secrets are accessed via
`Organization.get_secret(name)`, which fetches via name, not ID. **Flag-only**
— per scope, no new manager methods. The existing `get_secret(name)` flow
still works. (`POST /v1/secrets`, `PATCH /v1/secrets/{id}` are not exposed.)

---

## 8. `Region` ↔ `Region` / `RegionV2`

File: `singlestoredb/management/v1/region.py:12`

### Field coverage

Python `__init__` takes `name`, `provider`, `id`, `region_name`. Spec
`Region` has `provider`, `region` (mapped to `name`), `regionID` (mapped to
`id`). Spec `RegionV2` has `provider`, `region`, `regionName`.

| Spec field | Python | Status |
|---|---|---|
| `regionID` | `id` | present |
| `region` | `name` | present (mapped via `from_dict`) |
| `provider` | `provider` | present |
| `regionName` (V2 only) | `region_name` | present |

No missing fields. The class supports both V1 and V2 shapes via
`region_name`.

### Mistyped / under-parsed fields

| Field | Current | Spec | Proposed fix |
|---|---|---|---|
| `provider` | `str` | `CloudProvider` enum | **Flag-only** — keep as string for compat. |

---

## 9. `BillingUsage` (`UsageItem` + `BillingUsageItem`) ↔ inline billing usage

File: `singlestoredb/management/v1/billing_usage.py`

### Schema reminder

`GET /v1/billing/usage` returns `inline-object(billingUsage)` where
`billingUsage` is `array<BillingUsageItem>`. `BillingUsage` schema:

```
metric: enum(compute-hour)
usage: array<object>   # nested usage items
```

(Inner usage object has `startTime`, `endTime`, `ownerID`, `resourceID`,
`resourceName`, `resourceType`, `value` — these match `UsageItem`.)

### Bug already noted in PR review

`BillingUsageItem.from_dict` reads `obj['Usage']` (capital `U`). The actual
JSON key is lowercase `usage`. **This is a runtime bug.** Fix to
`obj.get('usage', obj.get('Usage', []))` for safety, or just `obj['usage']`.

### Field coverage on `UsageItem`

All 7 fields covered (`startTime`, `endTime`, `ownerID`, `resourceID`,
`resourceName`, `resourceType`, `value`).

### `BillingUsageItem` field coverage

Spec has `metric`, `usage`, plus a `description` field that the Python class
preserves. Coverage matches spec.

### Manager query params

`GET /v1/billing/usage` accepts `metric`, `startTime`, `endTime`,
`aggregateBy`. Verify the `Billing.usage()` method (in `workspace.py:1580`)
exposes all four. The signature was not surfaced in the audit reads —
**flag for verification** before editing.

---

## Cross-cutting flag-only items

These are not in the scope of this audit pass but are worth noting:

1. **`InferenceAPIInfo` ↔ `NotebookCloudFunction`** — class name does not match
   the spec's resource name. Class & endpoints look like cloud functions but
   are described as inference APIs. (Out of scope per task.)
2. **`smart_dr` / `backupBucketKMSKeyID` / `dataBucketKMSKeyID`** posted by
   `create_workspace_group` are not in the OpenAPI spec. Either undocumented,
   internal, or removed. Don't remove — flag for upstream confirmation.
3. **`fields=` query param** on every GET — intentionally skipped per scope.
4. **DR / identity / privateConnections / delegatedEntities sub-resources** on
   workspace groups — intentionally skipped per scope.

---

## Suggested edit ordering

If/when we move to code, edits in roughly this order minimize the chance of
test churn:

1. **Bug fixes first** (visible behavior changes):
   - `BillingUsageItem.from_dict` lowercase `usage` key (currently reads
     `obj['Usage']`).
   - `JobsManager.schedule` body: include
     `maxAllowedExecutionDurationInMinutes` in `executionConfig`.
2. **Field additions to existing `from_dict` parsers** (additive, low risk):
   - `Workspace`: `auto_scale`, `kai_enabled`, `scale_factor`.
   - `WorkspaceGroup`: `state`, `expires_at`, `update_window`,
     `deployment_type`, `high_availability_two_zones`,
     `opt_in_preview_feature`, `outbound_allow_list`, `project_id`,
     `project_name`, `smart_dr_status`.
   - `StarterWorkspace`: `mysql_dml_port`, `websocket_port`, `project_id`.
   - `Secret`: parse the three timestamps via `to_datetime`.
3. **Body-coverage additions** to manager create/update methods:
   - `WorkspaceManager.create_workspace`: `auto_scale`, `scale_factor`.
   - `WorkspaceGroup.create_workspace`: same passthrough.
   - `Workspace.update`: `auto_scale`, `enable_kai`, `scale_factor`.
   - `WorkspaceManager.create_workspace_group`: `provider`, `region_name`,
     `deployment_type`, `high_availability_two_zones`,
     `opt_in_preview_feature`, `project_id`.
   - `WorkspaceGroup.update`: `deployment_type`.
   - `WorkspaceManager.create_starter_workspace`: `project_id`.
4. **v2/ override layer** — v2 classes subclass v1 (per ADR 0001) and only
   override what differs. Adding fields in v1 propagates to v2 automatically
   through inheritance — **no v2 changes are needed for any work in this
   audit**.

---

## Working notes for implementation

This section is for whoever picks up the implementation in a fresh context.

### Source files

- **OpenAPI spec:** `management-openapi.txt` at the repo root (untracked, but
  on disk locally). Format: YAML with `paths:` and `components.schemas:`
  blocks. ~7300 lines.
- **Python wrappers:** `singlestoredb/management/v1/{workspace,job,files,
  organization,region,billing_usage}.py`.
- **ADR:** `docs/adr/0001-versioned-management-api-wrappers.md` covers the
  v1/v2 inheritance pattern.

### Conventions in this codebase

- All entity classes accept raw input in `__init__`; parsing/typing happens
  in `from_dict`. New `__init__` params are appended as
  `Optional[...] = None`. Callers go through `from_dict`, never construct
  directly.
- Manager classes (`WorkspaceManager`, `JobsManager`, `RegionManager`,
  `FilesManager`, `BillingManager`) use `self._get / _post / _put / _patch /
  _delete` from `Manager` base. `_post(json=...)` for JSON bodies.
- v2 classes inherit from v1 and override only what differs. **No v2 file
  changes are needed for this audit's work** — every new field added to a
  v1 `from_dict` is picked up by v2 automatically.
- Entity classes use the `VersionedMixin` for `.v1` / `.v2` attribute
  access. They store the raw response in `self._response` so version
  switching can re-parse.

### Utility functions to use

From `singlestoredb/management/utils.py`:

- `to_datetime(value)` — parses str → datetime, returns `None` for `None`
  (use for optional date fields like `terminatedAt`, `lastResumedAt`).
- `to_datetime_strict(value)` — required version, raises if `None`.
- `from_datetime(dt)` — datetime → ISO string (use when building POST
  bodies).
- `snake_to_camel_dict(d)` — converts snake_case dict keys to camelCase
  (used in `auto_suspend`, `update_window` POST bodies).
- `camel_to_snake_dict(d)` — reverse direction (used when storing nested
  response dicts on the entity).
- `stringify(x)`, `listify(x)` — defensive coercion for response values.

### Typical edit shape (additive field)

For `Workspace.kai_enabled` as a worked example:

```python
# 1. In __init__, append to the parameter list (keep alphabetical-ish
#    grouping if other recent params are at end):
def __init__(
    self,
    ...,
    last_resumed_at: Optional[Union[str, datetime.datetime]] = None,
    auto_scale: Optional[Dict[str, Any]] = None,
    kai_enabled: Optional[bool] = None,
    scale_factor: Optional[float] = None,
):
    ...
    self.auto_scale = auto_scale
    self.kai_enabled = kai_enabled
    self.scale_factor = scale_factor

# 2. In the class-level attribute annotations (just above __init__):
auto_scale: Optional[Dict[str, Any]]
kai_enabled: Optional[bool]
scale_factor: Optional[float]

# 3. In from_dict, append to the cls(...) call:
out = cls(
    ...,
    auto_scale=obj.get('autoScale'),
    kai_enabled=obj.get('kaiEnabled'),
    scale_factor=obj.get('scaleFactor'),
)
```

### Pre-commit gate (mandatory)

Per repo CLAUDE.md and personal CLAUDE.md: **run `pre-commit` before every
commit.** Stage → run → fix flagged issues → re-stage → re-run until
clean. The pipeline runs flake8, autopep8, reorder-python-imports,
add-trailing-comma, mypy, and standard whitespace hooks. Don't skip it.

### Tests

Tests live in `singlestoredb/tests/`. Management-API tests are marked
`@pytest.mark.management` and require a token. The auto-Docker test runner
will not exercise management endpoints — those need a real environment.
For this audit's edits, mypy + pre-commit are the minimum confidence bar;
manual verification against a real account is ideal but not blocking.
