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
#  This file was originally copied from https://github.com/sassoftware/python-swat.
#
"""
Generalized interface for configuring, setting, and getting options.

Options can be set and retrieved using set_option(...), get_option(...), and
reset_option(...).  The describe_option(...) function can be used to display
a description of one or more options.

"""
import contextlib
import os
import re
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterator
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple
from typing import Union
from urllib.parse import urlparse

from .xdict import xdict


# Container for options
_config = xdict()

items_types = (list, tuple, set)


def _getenv(names: Union[str, List[str]], *args: Any) -> str:
    """
    Check for multiple environment variable values.

    Two forms of the environment variable name will be checked,
    both with and without underscores.  This allows for aliases
    such as CAS_HOST and CASHOST.

    Parameters
    ----------
    names : str or list of str
        Names of environment variables to look for
    *args : any, optional
        The default return value if no matching environment
        variables exist

    Returns
    -------
    string or default value

    """
    if not isinstance(names, items_types):
        names = [names]
    for name in names:
        if name in os.environ:
            return os.environ[name]
        name = name.replace('_', '')
        if name in os.environ:
            return os.environ[name]
    if args:
        return args[0]
    raise KeyError(names[0])


def _setenv(names: Union[str, List[str]], value: Any) -> None:
    """
    Set environment variable.

    The environment is first checked for an existing variable
    that is set.  If it finds one, it uses that name.
    If no variable is found, the first one in the `names`
    list is used.

    Just as with _getenv, the variable name is checked both
    with and without underscores to allow aliases.

    Parameters
    ----------
    names : str or list of str
        Names of environment variable to look for
    value : Any
        The value to set

    """
    if not isinstance(names, items_types):
        names = [names]
    for name in names:
        if name in os.environ:
            os.environ[name] = value
        name = name.replace('_', '')
        if name in os.environ:
            os.environ[name] = value


def _delenv(names: Union[str, List[str]]) -> None:
    """Delete given environment variables."""
    if not isinstance(names, items_types):
        names = [names]
    for name in names:
        os.environ.pop(name, None)
        os.environ.pop(name.replace('_', ''), None)


def iteroptions(*args: Any, **kwargs: Any) -> Iterator[Tuple[str, Any]]:
    """
    Iterate through name / value pairs of options

    Options can come in several forms.  They can be consecutive arguments
    where the first argument is the name and the following argument is
    the value.  They can be two-element tuples (or lists) where the first
    element is the name and the second element is the value.  You can
    also pass in a dictionary of key / value pairs.  And finally, you can
    use keyword arguments.

    Parameters
    ----------
    *args : any, optional
        See description above.
    **kwargs : key / value pairs, optional
        Arbitrary keyword arguments.

    Returns
    -------
    generator
        Each iteration returns a name / value pair in a tuple

    """
    items = list(args)
    while items:
        item = items.pop(0)
        if isinstance(item, (list, tuple)):
            yield item[0], item[1]
        elif isinstance(item, dict):
            for key, value in item.items():
                yield key, value
        else:
            yield item, items.pop(0)
    for key, value in kwargs.items():
        yield key, value


@contextlib.contextmanager
def option_context(*args: Any, **kwargs: Any) -> Iterator[None]:
    """
    Create a context for setting option temporarily.

    Parameters
    ----------
    *args : str / any pairs
        Name / value pairs in consecutive arguments (not tuples)
    **kwargs : dict
        Key / value pairs of options

    """
    # Save old state and set new option values
    oldstate = {}
    for key, value in iteroptions(*args, **kwargs):
        key = key.lower()
        oldstate[key] = get_option(key)
        set_option(key, value)

    # Yield control
    yield

    # Set old state back
    for key, value in oldstate.items():
        set_option(key, value)


def _get_option_leaf_node(key: str) -> str:
    """
    Find full option name of given key.

    Parameters
    ----------
    key : str
        Either a partial key or full key name of an option

    Returns
    -------
    str
        The full key name of the option

    Raises
    ------
    KeyError
        If more than one option matches

    """
    flatkeys = list(_config.flatkeys())
    key = key.lower()
    if key in flatkeys:
        return key
    keys = [k for k in flatkeys if k.endswith('.' + key)]
    if len(keys) > 1:
        raise KeyError('There is more than one option with the name %s.' % key)
    if not keys:
        if '.' in key:
            raise KeyError('%s is not a valid option name.' % key)
        else:
            raise TypeError('%s is not a valid option name.' % key)
    return keys[0]


def set_option(*args: Any, **kwargs: Any) -> None:
    """
    Set the value of an option.

    Parameters
    ----------
    *args : str or Any
        The name and value of an option in consecutive arguments (not tuples)
    **kwargs : dict
        Arbitrary keyword / value pairs

    """
    for key, value in iteroptions(*args, **kwargs):
        key = _get_option_leaf_node(key)
        opt = _config[key]
        if not isinstance(opt, Option):
            raise TypeError('%s is not a valid option name' % key)
        opt.set(value)


set_options = set_option


def get_option(key: str) -> Any:
    """
    Get the value of an option.

    Parameters
    ----------
    key : str
        The name of the option

    Returns
    -------
    Any
        The value of the option

    """
    key = _get_option_leaf_node(key)
    opt = _config[key]
    if not isinstance(opt, Option):
        raise TypeError('%s is not a valid option name' % key)
    return opt.get()


def get_suboptions(key: str) -> Dict[str, Any]:
    """
    Get the dictionary of options at the level `key`.

    Parameters
    ----------
    key : str
        The name of the option collection

    Returns
    -------
    dict
        The dictionary of options at level `key`

    """
    if key not in _config:
        raise KeyError('%s is not a valid option name' % key)
    opt = _config[key]
    if isinstance(opt, Option):
        raise TypeError('%s does not have sub-options' % key)
    return opt


def get_default(key: str) -> Any:
    """
    Get the default value of an option.

    Parameters
    ----------
    key : str
        The name of the option

    Returns
    -------
    Any
        The default value of the option

    """
    key = _get_option_leaf_node(key)
    opt = _config[key]
    if not isinstance(opt, Option):
        raise TypeError('%s is not a valid option name' % key)
    return opt.get_default()


get_default_val = get_default


def describe_option(*keys: str, **kwargs: Any) -> Optional[str]:
    """
    Print the description of one or more options.

    To print the descriptions of all options, execute this function
    with no parameters.

    Parameters
    ----------
    *keys : one or more strings
        Names of the options

    """
    _print_desc = kwargs.get('_print_desc', True)

    out = []

    if not keys:
        keys = tuple(sorted(_config.flatkeys()))
    else:
        newkeys = []
        for k in keys:
            try:
                newkeys.append(_get_option_leaf_node(k))
            except (KeyError, TypeError):
                newkeys.append(k)

    for key in keys:

        if key not in _config:
            raise KeyError('%s is not a valid option name' % key)

        opt = _config[key]
        if isinstance(opt, xdict):
            desc = describe_option(
                *[
                    '%s.%s' % (key, x)
                    for x in opt.flatkeys()
                ], _print_desc=_print_desc,
            )
            if desc is not None:
                out.append(desc)
            continue

        if _print_desc:
            print(opt.__doc__)
            print('')
        else:
            out.append(opt.__doc__)

    if not _print_desc:
        return '\n'.join(out)

    return None


def reset_option(*keys: str) -> None:
    """
    Reset one or more options back to their default value.

    Parameters
    ----------
    *keys : one or more strings
        Names of options to reset

    """
    if not keys:
        keys = tuple(sorted(_config.flatkeys()))
    else:
        keys = tuple([_get_option_leaf_node(k) for k in keys])

    for key in keys:

        if key not in _config:
            raise KeyError('%s is not a valid option name' % key)

        opt = _config[key]
        if not isinstance(opt, Option):
            raise TypeError('%s is not a valid option name' % key)

        # Reset options
        set_option(key, get_default(key))


def check_int(
    value: Union[int, float, str],
    minimum: Optional[int] = None,
    maximum: Optional[int] = None,
    exclusive_minimum: bool = False,
    exclusive_maximum: bool = False,
    multiple_of: Optional[int] = None,
) -> int:
    """
    Validate an integer value.

    Parameters
    ----------
    value : int or float
        Value to validate
    minimum : int, optional
        The minimum value allowed
    maximum : int, optional
        The maximum value allowed
    exclusive_minimum : bool, optional
        Should the minimum value be excluded as an endpoint?
    exclusive_maximum : bool, optional
        Should the maximum value be excluded as an endpoint?
    multiple_of : int, optional
        If specified, the value must be a multple of it in order for
        the value to be considered valid.

    Returns
    -------
    int
        The validated integer value

    """
    out = int(value)

    if minimum is not None:
        if out < minimum:
            raise ValueError(
                '%s is smaller than the minimum value of %s' %
                (out, minimum),
            )
        if exclusive_minimum and out == minimum:
            raise ValueError(
                '%s is equal to the exclusive nimum value of %s' %
                (out, minimum),
            )

    if maximum is not None:
        if out > maximum:
            raise ValueError(
                '%s is larger than the maximum value of %s' %
                (out, maximum),
            )
        if exclusive_maximum and out == maximum:
            raise ValueError(
                '%s is equal to the exclusive maximum value of %s' %
                (out, maximum),
            )

    if multiple_of is not None and (out % int(multiple_of)) != 0:
        raise ValueError('%s is not a multiple of %s' % (out, multiple_of))

    return out


def check_float(
    value: Union[float, int, str],
    minimum: Optional[Union[float, int]] = None,
    maximum: Optional[Union[float, int]] = None,
    exclusive_minimum: bool = False,
    exclusive_maximum: bool = False,
    multiple_of: Optional[Union[float, int]] = None,
) -> float:
    """
    Validate a floating point value.

    Parameters
    ----------
    value : int or float
        Value to validate
    minimum : int or float, optional
        The minimum value allowed
    maximum : int or float, optional
        The maximum value allowed
    exclusive_minimum : bool, optional
        Should the minimum value be excluded as an endpoint?
    exclusive_maximum : bool, optional
        Should the maximum value be excluded as an endpoint?
    multiple_of : int or float, optional
        If specified, the value must be a multple of it in order for
        the value to be considered valid.

    Returns
    -------
    float
        The validated floating point value

    """
    out = float(value)

    if minimum is not None:
        if out < minimum:
            raise ValueError(
                '%s is smaller than the minimum value of %s' %
                (out, minimum),
            )
        if exclusive_minimum and out == minimum:
            raise ValueError(
                '%s is equal to the exclusive nimum value of %s' %
                (out, minimum),
            )

    if maximum is not None:
        if out > maximum:
            raise ValueError(
                '%s is larger than the maximum value of %s' %
                (out, maximum),
            )
        if exclusive_maximum and out == maximum:
            raise ValueError(
                '%s is equal to the exclusive maximum value of %s' %
                (out, maximum),
            )

    if multiple_of is not None and (out % int(multiple_of)) != 0:
        raise ValueError('%s is not a multiple of %s' % (out, multiple_of))

    return out


def check_bool(value: Union[bool, int]) -> bool:
    """
    Validate a bool value.

    Parameters
    ----------
    value : int or bool
        The value to validate.  If specified as an integer, it must
        be either 0 for False or 1 for True.

    Returns
    -------
    bool
        The validated bool

    """
    if value is False or value is True:
        return value

    if isinstance(value, int):
        if value == 1:
            return True
        if value == 0:
            return False

    if isinstance(value, (str, bytes)):
        value = str(value)
        if value.lower() in ['y', 'yes', 'on', 't', 'true', 'enable', 'enabled', '1']:
            return True
        if value.lower() in ['n', 'no', 'off', 'f', 'false', 'disable', 'disabled', '0']:
            return False

    raise ValueError('%s is not a recognized bool value')


def check_optional_bool(value: Optional[Union[bool, int]]) -> Optional[bool]:
    """
    Validate an optional bool value.

    Parameters
    ----------
    value : int or bool or None
        The value to validate.  If specified as an integer, it must
        be either 0 for False or 1 for True.

    Returns
    -------
    bool
        The validated bool

    """
    if value is None:
        return None

    return check_bool(value)


def check_str(
    value: Any,
    pattern: Optional[str] = None,
    max_length: Optional[int] = None,
    min_length: Optional[int] = None,
    valid_values: Optional[List[str]] = None,
) -> Optional[str]:
    """
    Validate a string value.

    Parameters
    ----------
    value : string
        The value to validate
    pattern : regular expression string, optional
        A regular expression used to validate string values
    max_length : int, optional
        The maximum length of the string
    min_length : int, optional
        The minimum length of the string
    valid_values : list of strings, optional
        List of the only possible values

    Returns
    -------
    string
        The validated string value

    """
    if value is None:
        return None

    if isinstance(value, str):
        out = value
    else:
        out = str(value, 'utf-8')

    if max_length is not None and len(out) > max_length:
        raise ValueError(
            '%s is longer than the maximum length of %s' %
            (out, max_length),
        )

    if min_length is not None and len(out) < min_length:
        raise ValueError(
            '%s is shorter than the minimum length of %s' %
            (out, min_length),
        )

    if pattern is not None and not re.search(pattern, out):
        raise ValueError('%s does not match pattern %s' % (out, pattern))

    if valid_values is not None and out not in valid_values:
        raise ValueError(
            '%s is not one of the possible values: %s' %
            (out, ', '.join(valid_values)),
        )

    return out


def check_dict_str_str(
    value: Any,
) -> Optional[Dict[str, str]]:
    """
    Validate a string value.

    Parameters
    ----------
    value : dict
        The value to validate. Keys and values must be strings.

    Returns
    -------
    dict
        The validated dict value
    """
    if value is None:
        return None

    if not isinstance(value, Mapping):
        raise ValueError(
            'value {} must be of type dict'.format(value),
        )

    out = {}
    for k, v in value.items():
        if not isinstance(k, str) or not isinstance(v, str):
            raise ValueError(
                'keys and values in {} must be strings'.format(value),
            )
        out[k] = v

    return out


def check_url(
    value: str,
    pattern: Optional[str] = None,
    max_length: Optional[int] = None,
    min_length: Optional[int] = None,
    valid_values: Optional[List[str]] = None,
) -> Optional[str]:
    """
    Validate a URL value.

    Parameters
    ----------
    value : any
        The value to validate.  This value will be cast to a string
        and converted to unicode.
    pattern : regular expression string, optional
        A regular expression used to validate string values
    max_length : int, optional
        The maximum length of the string
    min_length : int, optional
        The minimum length of the string
    valid_values : list of strings, optional
        List of the only possible values

    Returns
    -------
    string
        The validated URL value

    """
    if value is None:
        return None

    out = check_str(
        value, pattern=pattern, max_length=max_length,
        min_length=min_length, valid_values=valid_values,
    )
    try:
        urlparse(out)
    except Exception:
        raise TypeError('%s is not a valid URL' % value)
    return out


class Option(object):
    """
    Configuration option.

    Parameters
    ----------
    name : str
        The name of the option
    typedesc : str
        Description of the option data type (e.g., int, float, string)
    validator : callable
        A callable object that validates the option value and returns
        the validated value.
    default : any
        The default value of the option
    doc : str
        The documentation string for the option
    environ : str or list of strs, optional
        If specified, the value should be specified in an environment
        variable of that name.

    """

    def __init__(
        self,
        name: str,
        typedesc: str,
        validator: Callable[[str], Any],
        default: Any,
        doc: str,
        environ: Optional[Union[str, List[str]]] = None,
    ):
        self._name = name
        self._typedesc = typedesc
        self._validator = validator
        if environ is not None:
            self._default = validator(_getenv(environ, default))
        else:
            self._default = validator(default)
        self._environ = environ
        self._value = self._default
        self._doc = doc

    @property
    def __doc__(self) -> str:  # type: ignore
        """Generate documentation string."""
        separator = ' '
        if isinstance(self._value, (str, bytes)) and len(self._value) > 40:
            separator = '\n    '
        return '''%s : %s\n    %s\n    [default: %s]%s[currently: %s]\n''' % \
            (
                self._name, self._typedesc, self._doc.rstrip().replace('\n', '\n    '),
                self._default, separator, str(self._value),
            )

    def set(self, value: Any) -> None:
        """
        Set the value of the option.

        Parameters
        ----------
        value : any
           The value to set

        """
        value = self._validator(value)
        _config[self._name]._value = value

        if self._environ is not None:
            if value is None:
                _delenv(self._environ)
            else:
                _setenv(self._environ, str(value))

    def get(self) -> Any:
        """
        Get the value of the option.

        Returns
        -------
        Any
            The value of the option

        """
        if self._environ is not None:
            try:
                _config[self._name]._value = self._validator(_getenv(self._environ))
            except KeyError:
                pass
        return _config[self._name]._value

    def get_default(self) -> Any:
        """
        Get the default value of the option.

        Returns
        -------
        Any
            The default value of the option

        """
        return _config[self._name]._default


def register_option(
    key: str,
    typedesc: str,
    validator: Callable[[Any], Any],
    default: Any,
    doc: str,
    environ: Optional[Union[str, List[str]]] = None,
) -> None:
    """
    Register a new option.

    Parameters
    ----------
    key : str
        The name of the option
    typedesc : str
        Description of option data type (e.g., int, float, string)
    validator : callable
        A callable object that validates the value and returns
        a validated value.
    default : any
        The default value of the option
    doc : str
        The documentation string for the option
    environ : str or list of strs, optional
        If specified, the value should be specified in an environment
        variable of that name.

    """
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        _config[key] = Option(key, typedesc, validator, default, doc, environ=environ)


class AttrOption(object):
    """
    Attribute-style access of options.

    Parameters
    ----------
    name : str
        Name of the option

    """

    def __init__(self, name: str):
        object.__setattr__(self, '_name', name)

    def __dir__(self) -> List[str]:
        """Return list of flattened keys."""
        if self._name in _config:
            return _config[self._name].flatkeys()
        return _config.flatkeys()

    @property
    def __doc__(self) -> Optional[str]:  # type: ignore
        if self._name:
            return describe_option(self._name, _print_desc=False)
        return describe_option(_print_desc=False)

    def __getattr__(self, name: str) -> Any:
        """
        Retieve option as an attribute.

        Parameters
        ----------
        name : str
            Name of the option

        Returns
        -------
        Any

        """
        name = name.lower()
        if self._name:
            fullname = self._name + '.' + name
        else:
            fullname = name
        if fullname not in _config:
            fullname = _get_option_leaf_node(fullname)
        out = _config[fullname]
        if not isinstance(out, Option):
            return type(self)(fullname)
        return out.get()

    def __setattr__(self, name: str, value: Any) -> Any:
        """
        Set an attribute value.

        Parameters
        ----------
        name : str
            Name of the option
        value : Any
            Value of the option

        """
        name = name.lower()
        if self._name:
            fullname = self._name + '.' + name
        else:
            fullname = name
        if fullname not in _config:
            fullname = _get_option_leaf_node(fullname)
        out = _config[fullname]
        if not isinstance(out, Option):
            return type(self)(fullname)
        _config[fullname].set(value)
        return

    def __call__(self, *args: Any, **kwargs: Any) -> Iterator[None]:
        """Shortcut for option context."""
        return option_context(*args, **kwargs)  # type: ignore


# Object for setting and getting options using attribute syntax
options = AttrOption('')
