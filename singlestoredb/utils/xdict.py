#!/usr/bin/env python
#
# Copyright SAS Institute
#
#  Licensed under the Apache License, Version 2.0 (the License);
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#  This file originally copied from https://github.com/sassoftware/python-swat
#
"""Dictionary that allows setting nested keys by period (.) delimited strings."""
import copy
import re
from collections.abc import ItemsView
from collections.abc import Iterable
from collections.abc import KeysView
from collections.abc import ValuesView
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple


def _is_compound_key(key: str) -> bool:
    """
    Check for a compound key name.

    Parameters
    ----------
    key : string
        The key name to check

    Returns
    -------
    True
        If the key is compound (i.e., contains a '.')
    False
        If the key is not compound

    """
    return isinstance(key, str) and '.' in key


class xdict(Dict[str, Any]):
    """
    Nested dictionary that allows setting of nested keys using '.' delimited strings.

    Keys with a '.' in them automatically get split into separate keys.
    Each '.' in a key represents another level of nesting in the resulting
    dictionary.

    Parameters
    ----------
    *args, **kwargs : Arbitrary arguments and keyword arguments
        Same arguments as `dict`

    Returns
    -------
    xdict object

    Examples
    --------
    >>> dct = xdict()
    >>> dct['a.b.c'] = 100
    {'a': {'b': {'c': 100}}}

    """

    _dir: List[str]

    def __init__(self, *args: Any, **kwargs: Any):
        super(xdict, self).__init__()
        self.update(*args, **kwargs)

    def __dir__(self) -> Iterable[str]:
        """Return keys in the dict."""
        if hasattr(self, '_dir') and self._dir:
            return list(self._dir)
        return super(xdict, self).__dir__()

    def set_dir_values(self, values: List[str]) -> None:
        """
        Set the valid values for keys to display in tab-completion.

        Parameters
        ----------
        values : list of strs
            The values to display

        """
        super(xdict, self).__setattr__('_dir', values)

    def set_doc(self, docstring: str) -> None:
        """Set the docstring for the xdict."""
        super(xdict, self).__setattr__('__doc__', docstring)

    def __copy__(self) -> 'xdict':
        """Return a copy of self."""
        return type(self)(**self)

    def __deepcopy__(self, memo: Any) -> 'xdict':
        """Return a deep copy of self."""
        out = type(self)()
        for key, value in self.items():
            if isinstance(value, (dict, list, tuple, set)):
                value = copy.deepcopy(value)
            out[key] = value
        return out

    @classmethod
    def from_json(cls, jsonstr: str) -> 'xdict':
        """
        Create an xdict object from a JSON string.

        Parameters
        ----------
        jsonstr : string
           Valid JSON string that represents an object

        Returns
        -------
        xdict object

        """
        import json
        out = cls()
        out.update(json.loads(jsonstr))
        return out

    def __setitem__(self, key: str, value: Any) -> Any:
        """Set a key/value pair in an xdict object."""
        if isinstance(value, dict) and not isinstance(value, type(self)):
            value = type(self)(value)
        if _is_compound_key(key):
            return self._xset(key, value)
        return super(xdict, self).__setitem__(key, value)

    def _xset(self, key: str, value: Any) -> Any:
        """
        Set a key/value pair allowing nested levels in the key.

        Parameters
        ----------
        key : any
           Key value, if it is a string delimited by periods (.), each
           period represents another level of nesting of xdict objects.
        value : any
           Data value

        """
        if isinstance(value, dict) and not isinstance(value, type(self)):
            value = type(self)(value)
        if _is_compound_key(key):
            current, key = key.split('.', 1)
            if current not in self:
                self[current] = type(self)()
            return self[current]._xset(key, value)
        self[key] = value
        return None

    def setdefault(self, key: str, *default: Any) -> Any:
        """Return keyed value, or set it to `default` if missing."""
        if _is_compound_key(key):
            try:
                return self[key]
            except KeyError:
                if default:
                    new_default = default[0]
                    if isinstance(default, dict) and not isinstance(default, type(self)):
                        new_default = type(self)(default)
                else:
                    new_default = None
                self[key] = new_default
                return new_default
        return super(xdict, self).setdefault(key, *default)

    def __contains__(self, key: object) -> bool:
        """Does the xdict contain `key`?."""
        if super(xdict, self).__contains__(key):
            return True
        return key in self.allkeys()

    has_key = __contains__

    def __getitem__(self, key: str) -> Any:
        """Get value stored at `key`."""
        if _is_compound_key(key):
            return self._xget(key)
        return super(xdict, self).__getitem__(key)

    def _xget(self, key: str, *default: Any) -> Any:
        """
        Return keyed value, or `default` if missing

        Parameters
        ----------
        key : any
           Key to look up
        *default : any
           Default value to return if key is missing

        Returns
        -------
        Any

        """
        if _is_compound_key(key):
            current, key = key.split('.', 1)
            try:
                return self[current]._xget(key)
            except KeyError:
                if default:
                    return default[0]
                raise KeyError(key)
        return self[key]

    def get(self, key: str, *default: Any) -> Any:
        """Return keyed value, or `default` if missing."""
        if _is_compound_key(key):
            return self._xget(key, *default)
        return super(xdict, self).get(key, *default)

    def __delitem__(self, key: str) -> Any:
        """Deleted keyed item."""
        if _is_compound_key(key):
            return self._xdel(key)
        super(xdict, self).__delitem__(key)

    def _xdel(self, key: str) -> Any:
        """
        Delete keyed item.

        Parameters
        ----------
        key : any
           Key to delete.  If it is a string that is period (.) delimited,
           each period represents another level of nesting of xdict objects.

        """
        if _is_compound_key(key):
            current, key = key.split('.', 1)
            try:
                return self[current]._xdel(key)
            except KeyError:
                raise KeyError(key)
        del self[key]

    def pop(self, key: str, *default: Any) -> Any:
        """Remove and return value stored at `key`."""
        try:
            out = self[key]
            del self[key]
            return out
        except KeyError:
            if default:
                return default[0]
            raise KeyError(key)

    def _flatten(
        self,
        dct: Dict[str, Any],
        output: Dict[str, Any],
        prefix: str = '',
    ) -> None:
        """
        Create a new dict with keys flattened to period (.) delimited keys

        Parameters
        ----------
        dct : dict
           The dictionary to flatten
        output : dict
           The resulting dictionary (used internally in recursion)
        prefix : string
           Key prefix built from upper levels of nesting

        Returns
        -------
        dict

        """
        if prefix:
            prefix = prefix + '.'
        for key, value in dct.items():
            if isinstance(value, dict):
                if isinstance(key, int):
                    intkey = '%s[%s]' % (re.sub(r'\.$', r'', prefix), key)
                    self._flatten(value, prefix=intkey, output=output)
                else:
                    self._flatten(value, prefix=prefix + key, output=output)
            else:
                if isinstance(key, int):
                    intkey = '%s[%s]' % (re.sub(r'\.$', r'', prefix), key)
                    output[intkey] = value
                else:
                    output[prefix + key] = value

    def flattened(self) -> Dict[str, Any]:
        """Return an xdict with keys flattened to period (.) delimited strings."""
        output: Dict[str, Any] = {}
        self._flatten(self, output)
        return output

    def allkeys(self) -> List[str]:
        """Return a list of all possible keys (even sub-keys) in the xdict."""
        out = set()
        for key in self.flatkeys():
            out.add(key)
            while '.' in key:
                key = key.rsplit('.', 1)[0]
                out.add(key)
                if '[' in key:
                    out.add(re.sub(r'\[\d+\]', r'', key))
        return list(out)

    def flatkeys(self) -> List[str]:
        """Return a list of flattened keys in the xdict."""
        return list(self.flattened().keys())

    def flatvalues(self) -> List[Any]:
        """Return a list of flattened values in the xdict."""
        return list(self.flattened().values())

    def flatitems(self) -> List[Tuple[str, Any]]:
        """Return tuples of flattened key/value pairs."""
        return list(self.flattened().items())

    def iterflatkeys(self) -> Iterable[str]:
        """Return iterator of flattened keys."""
        return iter(self.flattened().keys())

    def iterflatvalues(self) -> Iterable[Any]:
        """Return iterator of flattened values."""
        return iter(self.flattened().values())

    def iterflatitems(self) -> Iterable[Tuple[str, Any]]:
        """Return iterator of flattened items."""
        return iter(self.flattened().items())

    def viewflatkeys(self) -> KeysView[str]:
        """Return view of flattened keys."""
        return self.flattened().keys()

    def viewflatvalues(self) -> ValuesView[Any]:
        """Return view of flattened values."""
        return self.flattened().values()

    def viewflatitems(self) -> ItemsView[str, Any]:
        """Return view of flattened items."""
        return self.flattened().items()

    def update(self, *args: Any, **kwargs: Any) -> None:
        """Merge the key/value pairs into `self`."""
        for arg in args:
            if isinstance(arg, dict):
                for key, value in arg.items():
                    self._xset(key, value)
            else:
                for key, value in arg:
                    self._xset(key, value)
        for key, value in kwargs.items():
            self._xset(key, value)

    def to_json(self) -> str:
        """
        Convert an xdict object to a JSON string.

        Returns
        -------
        str

        """
        import json
        return json.dumps(self)


class xadict(xdict):
    """An xdict that also allows setting/getting/deleting keys as attributes."""

    getdoc = None
    trait_names = None

    def _getAttributeNames(self) -> None:
        """Block this from creating attributes."""
        return

    def __delattr__(self, key: str) -> Any:
        """Delete the attribute stored at `key`."""
        if key.startswith('_') and key.endswith('_'):
            return super(xadict, self).__delattr__(key)
        del self[key]

    def __getattr__(self, key: str) -> Any:
        """Get the attribute store at `key`."""
        if key.startswith('_') and key.endswith('_'):
            return super(xadict, self).__getattr__(key)  # type: ignore
        try:
            return self[key]
        except KeyError:
            dct = type(self)()
            self[key] = dct
            return dct
        return None

    def __getitem__(self, key: str) -> Any:
        """Get item of an integer creates a new dict."""
        if isinstance(key, int) and key not in self:
            out = type(self)()
            self[key] = out
            return out
        return super(xadict, self).__getitem__(key)

    def __setattr__(self, key: str, value: Any) -> Any:
        """Set the attribute stored at `key`."""
        if key.startswith('_') and key.endswith('_'):
            return super(xadict, self).__setattr__(key, value)
        self[key] = value
