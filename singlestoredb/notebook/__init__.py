#!/usr/bin/env python
import functools
from typing import Any

from ..management import workspace as _ws


class Stage(_ws.Stage):
    """Stage manager."""

    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        # We are remapping the methods and attributes here so that
        # autocomplete still works in Jupyter / IPython, but we
        # bypass the real method / attribute calls and apply them
        # to the currently selected workspace group.
        for method in dir(_ws.Stage):
            if method.startswith('_'):
                continue
            attr = getattr(_ws.Stage, method)
            if callable(attr):
                def make_wrapper(m: str) -> Any:
                    def wrap(self: Stage, *a: Any, **kw: Any) -> Any:
                        return getattr(_ws.get_stage(), m)(*a, **kw)
                    return functools.update_wrapper(wrap, attr)
            else:
                def make_wrapper(m: str) -> Any:
                    def wrap(self: Stage) -> Any:
                        return getattr(_ws.get_stage(), m)
                    return functools.update_wrapper(wrap, attr)
            setattr(cls, method, make_wrapper(m=method))
        return super().__new__(cls, *args, **kwargs)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # Don't call super. We are proxying all method calls and
        # attribute accesses above.
        pass


class Secrets(object):
    """Wrapper for accessing secrets as object attributes."""

    def __getattr__(self, name: str) -> str:
        if name.startswith('_ipython') or name.startswith('_repr_'):
            raise AttributeError(name)
        return _ws.get_secret(name)


stage = Stage()
secrets = Secrets()

__all__ = ['stage', 'secrets']
