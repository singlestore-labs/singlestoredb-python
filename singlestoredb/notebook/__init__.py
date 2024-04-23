#!/usr/bin/env python
from typing import Any
from typing import List

from ..management import workspace as _ws


class Secrets(object):
    """Wrapper for accessing secrets as object attributes."""

    def __getattr__(self, name: str) -> str:
        if name.startswith('_ipython') or name.startswith('_repr_'):
            raise AttributeError(name)
        return _ws.get_secret(name)


def __getattr__(name: str) -> Any:
    if name == 'stage':
        return _ws.get_stage()

    elif name == 'secrets':
        return Secrets()

    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


def __dir__() -> List[str]:
    return list(sorted(__all__))


__all__ = ['stage', 'secrets']
