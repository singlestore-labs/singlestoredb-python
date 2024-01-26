#!/usr/bin/env python
"""SingleStoreDB Cluster Management."""
import datetime
import functools
import os
import re
import sys
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import SupportsIndex
from typing import TypeVar
from typing import Union
from urllib.parse import urlparse

import jwt

from .. import converters
from ..config import get_option

JSON = Union[str, List[str], Dict[str, 'JSON']]
JSONObj = Dict[str, JSON]
JSONList = List[JSON]
T = TypeVar('T')

if sys.version_info < (3, 10):
    PathLike = Union[str, os.PathLike]
    PathLikeABC = os.PathLike
else:
    PathLike = Union[str, os.PathLike[str]]
    PathLikeABC = os.PathLike[str]


class TTLProperty(object):
    """Property with time limit."""

    def __init__(self, fget: Callable[[Any], Any], ttl: datetime.timedelta):
        self.fget = fget
        self.ttl = ttl
        self._last_executed = datetime.datetime(2000, 1, 1)
        self._last_result = None
        self.__doc__ = fget.__doc__
        self._name = ''

    def reset(self) -> None:
        self._last_executed = datetime.datetime(2000, 1, 1)
        self._last_result = None

    def __set_name__(self, owner: Any, name: str) -> None:
        self._name = name

    def __get__(self, obj: Any, objtype: Any = None) -> Any:
        if obj is None:
            return self

        if self._last_result is not None \
                and (datetime.datetime.now() - self._last_executed) < self.ttl:
            return self._last_result

        self._last_result = self.fget(obj)
        self._last_executed = datetime.datetime.now()

        return self._last_result


def ttl_property(ttl: datetime.timedelta) -> Callable[[Any], Any]:
    """Property with a time-to-live."""
    def wrapper(func: Callable[[Any], Any]) -> Any:
        out = TTLProperty(func, ttl=ttl)
        return functools.wraps(func)(out)
    return wrapper


class NamedList(List[T]):
    """List class which also allows selection by ``name`` and ``id`` attribute."""

    def _find_item(self, key: str) -> T:
        for item in self:
            if getattr(item, 'name', '') == key:
                return item
            if getattr(item, 'id', '') == key:
                return item
        raise KeyError(key)

    def __getitem__(self, key: Union[SupportsIndex, slice, str]) -> Any:
        if isinstance(key, str):
            return self._find_item(key)
        return super().__getitem__(key)

    def __contains__(self, key: Any) -> bool:
        if isinstance(key, str):
            try:
                self._find_item(key)
                return True
            except KeyError:
                return False
        return super().__contains__(key)

    def names(self) -> List[str]:
        """Return ``name`` attribute of each item."""
        return [y for y in [getattr(x, 'name', None) for x in self] if y is not None]

    def ids(self) -> List[str]:
        """Return ``id`` attribute of each item."""
        return [y for y in [getattr(x, 'id', None) for x in self] if y is not None]

    def get(self, name_or_id: str, *default: Any) -> Any:
        """Return object with name / ID if it exists, or return default value."""
        try:
            return self._find_item(name_or_id)
        except KeyError:
            if default:
                return default[0]
            raise


def get_token() -> Optional[str]:
    """Return the token for the Management API."""
    # See if an API key is configured
    tok = get_option('management.token')
    if tok:
        return tok

    url = os.environ.get('SINGLESTOREDB_URL')
    if not url:
        # See if the connection URL contains a JWT
        url = get_option('host')
        if not url:
            return None

    urlp = urlparse(url, scheme='singlestoredb', allow_fragments=True)
    if urlp.password:
        try:
            jwt.decode(urlp.password, options={'verify_signature': False})
            return urlp.password
        except jwt.DecodeError:
            pass

    # Didn't find a key anywhere
    return None


def get_organization() -> Optional[str]:
    """Return the organization for the current token or environment."""
    org = os.environ.get('SINGLESTOREDB_ORGANIZATION')
    if org:
        return org

    return None


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
