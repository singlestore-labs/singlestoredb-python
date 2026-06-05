# ADR 0001: Versioned Management API Wrappers

## Status

Accepted

## Context

The Management API has multiple versions (v1, v2, etc.) with differing endpoints and response shapes. Previously, a single `Manager` instance was locked to one version via its `_base_url`, and all entities created through that manager used the same version. There was no way to access a different API version without creating a completely separate manager from scratch.

We needed a way to:
- Access specific versions of API wrappers from an existing manager
- Switch versions on entity objects (e.g., call a v2 endpoint from a v1 Workspace)
- Keep backward compatibility with existing import paths and usage patterns
- Allow v2 to incrementally override v1 behavior without duplicating everything

## Decision

### Folder structure

Versioned modules live in `management/v1/`, `management/v2/`, etc. Each version folder is a **complete set** — every class that should be accessible in that version must exist in its folder. There is no cross-version fallback; requesting a class from a version where it doesn't exist raises an error.

Top-level modules (`management/workspace.py`, etc.) become thin re-export shims that always import from v1 for stable import paths. Dynamic version routing (controlled by `config.get_option('management.version')`) happens in the `manage_*()` factory functions.

Shared infrastructure (`manager.py`, `utils.py`, `versioned.py`) stays at the top level outside version folders.

### Inheritance model

v2 classes subclass their v1 counterparts and override only what differs. Classes unchanged in v2 are imported from v1 and re-exported:

```python
# v2/workspace.py
from ..v1.workspace import Workspace as Workspace  # unchanged
from ..v1.workspace import WorkspaceGroup as _WorkspaceGroup

class WorkspaceGroup(_WorkspaceGroup):
    def new_v2_method(self):
        ...
```

### Version switching via VersionedMixin

A `VersionedMixin` class (in `management/versioned.py`) provides `__getattr__` that intercepts attribute access matching `v\d+` (e.g., `.v1`, `.v2`). Both `Manager` and entity classes use this mixin.

- **Managers**: `mgr.v2` returns a new manager of the same type from the v2 module, constructed with the same credentials but pointed at the v2 API URL. Cached on first access.
- **Entities**: `ws.v2` asks its `_manager` for a cached versioned manager clone, then constructs the target entity class via `from_dict(self._response, versioned_manager)`. Also cached.

### Convention-based module lookup

Version switching uses dynamic import based on conventions:
- Module name derived from `self.__class__.__module__.rsplit('.', 1)[-1]` (e.g., `'workspace'`)
- Class name derived from `type(self).__name__` (e.g., `'WorkspaceManager'`)
- Import path: `singlestoredb.management.{version}.{module_name}`

No registry or registration is needed — the folder structure is the registry.

### Credential storage

`Manager.__init__` stores `_access_token`, `_base_url_root`, and `_organization_id` so versioned clones can be constructed without re-fetching tokens.

### API version in URL

Each manager class has an `api_version` class attribute (defaults to `'v1'` on the base `Manager`). The URL is built as `urljoin(base_url_root, api_version) + '/'`. The `version` constructor parameter overrides this for dynamic version selection.

### Response storage

Entities store the raw API response dict as `self._response` in `from_dict()`. This enables version switching without re-fetching — the target version's `from_dict` reconstructs from the stored data, ignoring fields it doesn't understand.

## Alternatives Considered

### Single manager with version parameter per method call

Rejected: would pollute every method signature and make it unclear which version's response schema applies to the returned entity.

### Separate, unrelated manager classes per version

Rejected: massive code duplication. The inheritance model (v2 subclasses v1) keeps overrides minimal.

### Fallback to v1 if a class doesn't exist in v2

Rejected: silent fallback hides bugs. If you ask for `v2.SomeClass` and it doesn't exist, that's an error worth surfacing.

### Proxy objects instead of new instances for version switching

Rejected: adds a layer of indirection that makes type checking harder and debugging confusing. Concrete instances are simpler.

## Consequences

- Adding a new API version means creating a new folder and re-exporting (or overriding) each class
- Every entity class must store `_response` in `from_dict`, adding minor memory overhead
- Import paths are stable — existing code using `from singlestoredb.management.workspace import Workspace` continues to work unchanged
- The `VersionedMixin.__getattr__` only activates on `v\d+` patterns, so it doesn't interfere with normal attribute access
