#!/usr/bin/env python
import functools
from typing import Any
from typing import Optional

from .. import management as _mgmt
from ..management import workspace as _ws
from ..management.organization import Organization as _OrganizationBase
from ..management.stage import Stage as _StageBase
# Still the v1 shim, and only for the two globals below that are v1 vocabulary
# outright: ``workspace`` and ``workspacegroup``. Management API v2 replaced
# both with the flat ``Cluster``, and there is no ``cluster`` notebook global
# to proxy to yet, so these cannot simply be repointed -- giving the notebook
# environment a cluster global is a port, not a version bump. The other three
# globals (``secrets``, ``stage``, ``organization``) go through ``_mgmt``,
# whose helpers dispatch on the ``management.version`` option; taking them from
# this shim pinned them to v1 no matter what the option said.
#
# Consequence for a v1 notebook environment: those three now follow the option,
# which defaults to v2, so such an environment has to set
# SINGLESTOREDB_MANAGEMENT_VERSION=v1. That is the cost of them being neutral at
# all -- pinned, v1 worked and v2 was simply broken.


class Secrets(object):
    """Wrapper for accessing secrets as object attributes."""

    def __getattr__(self, name: str) -> Optional[str]:
        if name.startswith('_ipython') or name.startswith('_repr_'):
            raise AttributeError(name)
        return _mgmt.get_secret(name)

    def __getitem__(self, name: str) -> Optional[str]:
        return _mgmt.get_secret(name)


class Stage(object):

    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        # We are remapping the methods and attributes here so that
        # autocomplete still works in Jupyter / IPython, but we
        # bypass the real method / attribute calls and apply them
        # to the currently selected stage.
        for name in [x for x in dir(_StageBase) if not x.startswith('_')]:
            if name in ['from_dict', 'refresh', 'update']:
                continue
            attr = getattr(_StageBase, name)

            def make_wrapper(m: str, is_method: bool = False) -> Any:
                if is_method:
                    def wrap(self: Stage, *a: Any, **kw: Any) -> Any:
                        return getattr(_mgmt.get_stage(), m)(*a, **kw)
                    return functools.update_wrapper(wrap, attr)
                else:
                    def wrap(self: Stage, *a: Any, **kw: Any) -> Any:
                        return getattr(_mgmt.get_stage(), m)
                    return property(functools.update_wrapper(wrap, attr))

            setattr(cls, name, make_wrapper(m=name, is_method=callable(attr)))

        for name in [
            x for x in _StageBase.__annotations__.keys()
            if not x.startswith('_')
        ]:

            def make_annotation_wrapper(m: str) -> Any:
                def wrap(self: Stage) -> Any:
                    return getattr(_mgmt.get_stage(), m)
                return property(wrap)

            setattr(cls, name, make_annotation_wrapper(m=name))

        cls.__doc__ = _StageBase.__doc__

        return super().__new__(cls, *args, **kwargs)


class WorkspaceGroup(object):

    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        # We are remapping the methods and attributes here so that
        # autocomplete still works in Jupyter / IPython, but we
        # bypass the real method / attribute calls and apply them
        # to the currently selected workspace group.
        for name in [x for x in dir(_ws.WorkspaceGroup) if not x.startswith('_')]:
            if name in ['from_dict', 'refresh', 'update']:
                continue

            attr = getattr(_ws.WorkspaceGroup, name)

            def make_wrapper(m: str, is_method: bool = False) -> Any:
                if is_method:
                    def wrap(self: WorkspaceGroup, *a: Any, **kw: Any) -> Any:
                        return getattr(_ws.get_workspace_group(), m)(*a, **kw)
                    return functools.update_wrapper(wrap, attr)
                else:
                    def wrap(self: WorkspaceGroup, *a: Any, **kw: Any) -> Any:
                        return getattr(_ws.get_workspace_group(), m)
                    return property(functools.update_wrapper(wrap, attr))

            setattr(cls, name, make_wrapper(m=name, is_method=callable(attr)))

        for name in [
            x for x in _ws.WorkspaceGroup.__annotations__.keys()
            if not x.startswith('_')
        ]:

            def make_annotation_wrapper(m: str) -> Any:
                def wrap(self: WorkspaceGroup) -> Any:
                    return getattr(_ws.get_workspace_group(), m)
                return property(wrap)

            setattr(cls, name, make_annotation_wrapper(m=name))

        cls.__doc__ = _ws.WorkspaceGroup.__doc__

        return super().__new__(cls, *args, **kwargs)

    def __str__(self) -> str:
        return _ws.get_workspace_group().__str__()

    def __repr__(self) -> str:
        return _ws.get_workspace_group().__repr__()


class Workspace(object):

    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        # We are remapping the methods and attributes here so that
        # autocomplete still works in Jupyter / IPython, but we
        # bypass the real method / attribute calls and apply them
        # to the currently selected workspace.
        for name in [x for x in dir(_ws.Workspace) if not x.startswith('_')]:
            if name in ['from_dict', 'refresh', 'update']:
                continue

            attr = getattr(_ws.Workspace, name)

            def make_wrapper(m: str, is_method: bool = False) -> Any:
                if is_method:
                    def wrap(self: Workspace, *a: Any, **kw: Any) -> Any:
                        return getattr(_ws.get_workspace(), m)(*a, **kw)
                    return functools.update_wrapper(wrap, attr)
                else:
                    def wrap(self: Workspace, *a: Any, **kw: Any) -> Any:
                        return getattr(_ws.get_workspace(), m)
                    return property(functools.update_wrapper(wrap, attr))

            setattr(cls, name, make_wrapper(m=name, is_method=callable(attr)))

        for name in [
            x for x in _ws.Workspace.__annotations__.keys()
            if not x.startswith('_')
        ]:

            def make_annotation_wrapper(m: str) -> Any:
                def wrap(self: Workspace) -> Any:
                    return getattr(_ws.get_workspace(), m)
                return property(wrap)

            setattr(cls, name, make_annotation_wrapper(m=name))

        cls.__doc__ = _ws.Workspace.__doc__

        return super().__new__(cls, *args, **kwargs)

    def __str__(self) -> str:
        return _ws.get_workspace().__str__()

    def __repr__(self) -> str:
        return _ws.get_workspace().__repr__()


class Organization(object):

    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        # We are remapping the methods and attributes here so that
        # autocomplete still works in Jupyter / IPython, but we
        # bypass the real method / attribute calls and apply them
        # to the currently selected organization.
        for name in [x for x in dir(_OrganizationBase) if not x.startswith('_')]:
            if name in ['from_dict', 'refresh', 'update']:
                continue

            attr = getattr(_OrganizationBase, name)

            def make_wrapper(m: str, is_method: bool = False) -> Any:
                if is_method:
                    def wrap(self: Organization, *a: Any, **kw: Any) -> Any:
                        return getattr(_mgmt.get_organization(), m)(*a, **kw)
                    return functools.update_wrapper(wrap, attr)
                else:
                    def wrap(self: Organization, *a: Any, **kw: Any) -> Any:
                        return getattr(_mgmt.get_organization(), m)
                    return property(functools.update_wrapper(wrap, attr))

            setattr(cls, name, make_wrapper(m=name, is_method=callable(attr)))

        for name in [
            x for x in _OrganizationBase.__annotations__.keys()
            if not x.startswith('_')
        ]:

            def make_annotation_wrapper(m: str) -> Any:
                def wrap(self: Organization) -> Any:
                    return getattr(_mgmt.get_organization(), m)
                return property(wrap)

            setattr(cls, name, make_annotation_wrapper(m=name))

        cls.__doc__ = _OrganizationBase.__doc__

        return super().__new__(cls, *args, **kwargs)

    def __str__(self) -> str:
        return _mgmt.get_organization().__str__()

    def __repr__(self) -> str:
        return _mgmt.get_organization().__repr__()


secrets = Secrets()
stage = Stage()
organization = Organization()
workspace_group = WorkspaceGroup()
workspace = Workspace()


__all__ = ['secrets', 'stage', 'workspace', 'workspace_group', 'organization']
