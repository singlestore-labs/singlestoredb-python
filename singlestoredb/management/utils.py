#!/usr/bin/env python
"""SingleStoreDB Cluster Management."""
import datetime
import os
import re
import sys
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from .. import converters

JSON = Union[str, List[str], Dict[str, 'JSON']]
JSONObj = Dict[str, JSON]
JSONList = List[JSON]

if sys.version_info < (3, 10):
    PathLike = Union[str, os.PathLike]
    PathLikeABC = os.PathLike
else:
    PathLike = Union[str, os.PathLike[str]]
    PathLikeABC = os.PathLike[str]


def enable_http_tracing() -> None:
    """Enable tracing of HTTP requests."""
    import logging
    import http.client as http_client
    http_client.HTTPConnection.debuglevel = 1
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    requests_log = logging.getLogger('requests.packages.urllib3')
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True


def to_datetime(
    obj: Optional[Union[str, datetime.datetime]],
) -> Optional[datetime.datetime]:
    """Convert string to datetime."""
    if not obj:
        return None
    if isinstance(obj, datetime.datetime):
        return obj
    if obj == '0001-01-01T00:00:00Z':
        return None
    obj = obj.replace('Z', '')
    # Fix datetimes with truncated zeros
    if '.' in obj:
        obj, micros = obj.split('.', 1)
        micros = micros + '0' * (6 - len(micros))
        obj = obj + '.' + micros
    out = converters.datetime_fromisoformat(obj)
    if isinstance(out, str):
        return None
    if isinstance(out, datetime.date):
        return datetime.datetime(out.year, out.month, out.day)
    return out


def from_datetime(
    obj: Union[str, datetime.datetime],
) -> Optional[str]:
    """Convert datetime to string."""
    if not obj:
        return None
    if isinstance(obj, str):
        return obj
    out = obj.isoformat()
    if not re.search(r'[A-Za-z]$', out):
        out = f'{out}Z'
    return out


def vars_to_str(obj: Any) -> str:
    """Render a string representation of vars(obj)."""
    attrs = []
    obj_vars = vars(obj)
    if 'name' in obj_vars:
        attrs.append('name={}'.format(repr(obj_vars['name'])))
    if 'id' in obj_vars:
        attrs.append('id={}'.format(repr(obj_vars['id'])))
    for name, value in sorted(obj_vars.items()):
        if name in ('name', 'id'):
            continue
        if not value or name.startswith('_'):
            continue
        attrs.append('{}={}'.format(name, repr(value)))
    return '{}({})'.format(type(obj).__name__, ', '.join(attrs))


def single_item(s: Any) -> Any:
    """Return only item if ``s`` is a list, otherwise return ``s``."""
    if isinstance(s, list):
        if len(s) != 1:
            raise ValueError('list must only contain a singleitem')
        return s[0]
    return s


def stringify(s: JSON) -> str:
    """Convert list of strings to single string."""
    if isinstance(s, (tuple, list)):
        if len(s) > 1:
            raise ValueError('list contains more than one item')
        return s[0]
    if isinstance(s, dict):
        raise TypeError('only strings and lists are valid arguments')
    return s


def listify(s: JSON) -> List[str]:
    """Convert string to list of strings."""
    if isinstance(s, (tuple, list)):
        return list(s)
    if isinstance(s, dict):
        raise TypeError('only strings and lists are valid arguments')
    return [s]


def listify_obj(s: JSON) -> List[JSONObj]:
    """Convert object to list of objects."""
    if isinstance(s, (tuple, list)):
        for item in s:
            if not isinstance(item, dict):
                raise TypeError('only dicts and lists of dicts are valid parameters')
        return list(s)  # type: ignore
    if not isinstance(s, dict):
        raise TypeError('only dicts and lists of dicts are valid parameters')
    return [s]


def _upper_match(m: Any) -> str:
    """Upper-case the first match group."""
    return m.group(1).upper()


def snake_to_camel(s: Optional[str], cap_first: bool = False) -> Optional[str]:
    """Convert snake-case to camel-case."""
    if s is None:
        return None
    out = re.sub(r'_[A-Za-z]', _upper_match, s.lower())
    if cap_first and out:
        return out[0].upper() + out[1:]
    return out


def camel_to_snake(s: Optional[str]) -> Optional[str]:
    """Convert camel-case to snake-case."""
    if s is None:
        return None
    out = re.sub(r'([A-Z]+)', r'_\1', s).lower()
    if out and out[0] == '_':
        return out[1:]
    return out
