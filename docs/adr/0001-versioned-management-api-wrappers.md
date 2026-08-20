# ADR 0001: Versioned Management API Wrappers

## Status

Accepted. Amended — the original decision included a cross-version bridge
(`VersionedMixin`, per-entity `_response` storage, manager clones) that has
since been removed. See [Revisions](#revisions) for what changed and why.

## Context

The Management API has multiple versions (v1, v2, etc.) with differing endpoints and response shapes. A `Manager` instance is locked to one version via its `_base_url`, and all entities created through that manager use that version.

v2 is not an additive revision of v1. Workspace groups and workspaces were replaced by a single flat `Cluster` resource, Stage moved from a top-level resource to one nested under the cluster, regions lost their IDs, and the `inferenceapis/` routes disappeared entirely. So "v2 is v1 plus overrides" is not true in general, and v1 is expected to be abandoned outright rather than maintained alongside v2.

We needed a way to:
- Serve both versions from one package while they overlap
- Let each version differ in behavior without duplicating what they share
- Keep backward compatibility with existing import paths and usage patterns
- Make retiring v1 a deletion rather than an excavation

## Decision

### Folder structure

Version-specific modules live in `management/v1/`, `management/v2/`, etc. Version-neutral implementations live in the top-level `management/` modules (`manager.py`, `stage.py`, `job.py`, `organization.py`, `region.py`, `files.py`, `utils.py`).

A module belongs at the top level when both versions share the implementation and the only difference is the URL the shared code is pointed at. Anything else — a different resource model, a different request or response shape, a route that exists at one version only — belongs in a version folder.

Each version folder is a **complete set** for the resources that version has: every class reachable at that version must be importable from its folder, whether as a real subclass or a re-export of the shared implementation. There is no cross-version fallback; requesting a class from a version where it doesn't exist raises an error.

**Rule 1: no cross-version imports, in either direction.** `management/v1/` must not import from `management/v2/` and vice versa. Shared code moves up to `management/`; it never travels sideways. This is what makes retiring a version an `rm -rf` of its folder plus removal of the back-compat shims, and it is enforced by `TestVersionPackagesAreIndependent` in `singlestoredb/tests/test_management_versioning.py` — an AST walk over each folder's imports plus a `sys.meta_path` blocker that imports every module of one version with the other forbidden.

Top-level modules also serve as thin re-export shims for stable import paths (`from singlestoredb.management.workspace import Workspace` still resolves to the v1 class). Version routing happens in the `manage_*()` factory functions, which live at the top level only — duplicating a factory into a version folder both invites the copies to drift and makes the folder un-deletable.

### Inheritance model

The shared base class carries the **newest** version's behavior. Older versions subclass it and override backward. So:

```python
# management/stage.py -- the shared base is level-set to v2
class Stage(FileLocation):
    def _fs_path(self, path=''):
        return f'clusters/{self._deployment_id}/stage/fs/{path}'

# v1/stage.py -- the backward override
class Stage(_Stage):
    def _fs_path(self, path=''):
        return f'stage/{self._deployment_id}/fs/{path}'
```

and `v2/stage.py` is a plain re-export. The direction matters: with v2 as the subclass, deleting `v1/` would strand the base class it inherits from. With v1 as the subclass, deleting `v1/` leaves the current behavior standing on its own.

Version differences are expressed as **class attributes on the shared class**, repointed by the version subclass, rather than as runtime `if version == ...` branches:

- `JobsManager._deployment_target_type`, `_starter_target_type`, `_legacy_cluster_target_type` — the `targetType` strings each version uses
- `Organization._jobs_manager_class`, `_inference_api_manager_class`
- `Organizations._organization_class` — so a v1 manager hands out a v1-configured organization
- `Stage._fs_path` — the one thing that differs about Stage

A resource that exists at one version only lives in that version's folder, and the shared base raises a `ManagementError` explaining the absence if the operation has no equivalent. `inference_api.py` is v1-only for this reason; `RegionManager.list_shared_tier_regions` raises from the shared base and is implemented only in `v1/region.py`.

### Convention-based module lookup

`_import_versioned_module(version, module_name)` in `management/_version_import.py` imports `singlestoredb.management.{version}.{module_name}`, distinguishing "this version is unsupported" from "this version has no such module" in its error message. The `manage_*()` factories are its only callers. No registry or registration is needed — the folder structure is the registry.

### API version in URL

Each manager class has a `default_version` class attribute, a literal on the class. It is **not** resolved from `config.get_option('management.version')` at import time: doing that let a v1-only class declare itself to be v2 whenever the option was set. The URL is built as `urljoin(base_url_root, version or type(self).default_version) + '/'`.

The `management.version` option is consulted by the `manage_*()` factories, not by the manager classes, and only for resources that exist at more than one version. `manage_workspaces()` ignores it: workspaces are v1-only, so a global preference for another version has nothing to say about them, and only an explicit `version=` argument is an error.

### Deprecation of the v1 grammar

`manage_workspaces()` and the workspace-group vocabulary are deprecated in favor of `manage_clusters()`. The deprecation warning lives in `manage_workspaces()`; the un-warned body is `_manage_workspaces_v1()`. Internal callers that are v1-only by design — Fusion handlers, the UDF `stage://` handling, the AI helpers — call the private form, so they do not emit a warning the caller can do nothing about.

## Alternatives Considered

### Single manager with version parameter per method call

Rejected: would pollute every method signature and make it unclear which version's response schema applies to the returned entity.

### v2 subclasses v1

Rejected: it inverts the dependency relative to the lifecycle. v1 is the version that goes away, so it must be the leaf. It also does not describe v2 honestly — a `Cluster` is not a `Workspace` with overrides.

### Separate, unrelated manager classes per version

Rejected: the versions genuinely share most of their surface (files, jobs, secrets, billing, the HTTP plumbing), and duplicating it would let the copies drift. Sharing a level-set base with backward overrides in `v1/` keeps one implementation of the common part without making either version depend on the other.

### Runtime `if version == 'v1'` branches in shared code

Rejected: it spreads version knowledge across every method that has any, and the branches survive the deletion of `v1/` as dead code that still reads as live. A class attribute puts the difference in one declaration, at the version that owns it.

### Fallback to v1 if a class doesn't exist in v2

Rejected: silent fallback hides bugs. If you ask for a v2 class and it doesn't exist, that's an error worth surfacing.

## Consequences

- Adding a new API version means creating a folder, moving the newest behavior into the shared base, and leaving a backward override in the now-older folder
- Retiring a version means deleting its folder and the shims that re-export from it; nothing else refers to it
- Import paths are stable — existing code using `from singlestoredb.management.workspace import Workspace` continues to work unchanged
- Version differences are declarations rather than control flow, so "what differs at v1?" is answerable by reading `v1/`
- Entities carry no stored API response, so an object cannot be re-interpreted as another version after the fact; getting a different version's view means asking that version's manager

## Revisions

The accepted decision originally included a cross-version bridge, removed in
full on the `versioned-management-api` branch:

- **`VersionedMixin` and `.v1`/`.v2` attribute switching.** A `__getattr__` intercepting `v\d+` let any manager or entity hop versions in place, returning a cached clone or a re-parsed entity. Removed: callers reach a version through the factory they call, and the bridge required exactly the cross-version coupling that rule 1 forbids. Nothing consumed it outside its own tests.
- **`_response` storage on every entity.** Entities stashed their raw API response so another version's `from_dict` could re-read it. Removed with the bridge — with v1 and v2 modeling different resources, re-parsing one version's payload as another was not meaningful anyway.
- **Clone-support state on `Manager`** (`_access_token`, `_base_url_root`, `_organization_id`) and the v1↔v2 field translators (`v1/_translate.py`, `v1/cluster.py`) existed only to feed the bridge, and went with it.
- **Inheritance direction inverted** from "v2 subclasses v1" to "shared base level-set to the newest version, `v1/` holds backward overrides", for the reasons in the alternatives above.
- **`default_version` resolved from the config option.** The original text described it as resolved from `config.get_option('management.version')`; it is a class literal, and making it dynamic was the bug that let a v1 class report itself as v2.
- **`management/versioned.py` renamed to `_version_import.py`**, since all that remains of it is the version-module importer.
