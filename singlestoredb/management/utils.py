#!/usr/bin/env python
"""SingleStoreDB Cluster Management."""
from __future__ import annotations

import datetime
from typing import Any
from typing import Optional
from typing import Union


def to_datetime(
    obj: Optional[Union[str, datetime.datetime]],
) -> Optional[datetime.datetime]:
    """Convert string to datetime."""
    if not obj:
        return None
    if isinstance(obj, datetime.datetime):
        return obj
    obj = obj.replace('Z', '')
    # Fix datetimes with truncated zeros
    if '.' in obj:
        obj, micros = obj.split('.', 1)
        micros = micros + '0' * (6 - len(micros))
        obj = obj + '.' + micros
    return datetime.datetime.fromisoformat(obj)


def vars_to_str(obj: Any) -> str:
    """Render a string representation of vars(obj)."""
    attrs = []
    for name, value in sorted(vars(obj).items()):
        if not value or name.startswith('_'):
            continue
        attrs.append('{}={}'.format(name, repr(value)))
    return '{}({})'.format(type(obj).__name__, ', '.join(attrs))
