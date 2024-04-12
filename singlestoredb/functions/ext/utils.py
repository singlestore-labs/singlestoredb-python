#!/usr/bin/env python
import json
import logging
import re
import sys
import zipfile
from copy import copy
from typing import Any
from typing import Dict
from typing import List
from typing import Union

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore

try:
    from uvicorn.logging import DefaultFormatter

except ImportError:

    class DefaultFormatter(logging.Formatter):  # type: ignore

        def formatMessage(self, record: logging.LogRecord) -> str:
            recordcopy = copy(record)
            levelname = recordcopy.levelname
            seperator = ' ' * (8 - len(recordcopy.levelname))
            recordcopy.__dict__['levelprefix'] = levelname + ':' + seperator
            return super().formatMessage(recordcopy)


def get_logger(name: str) -> logging.Logger:
    """Return a new logger."""
    logger = logging.getLogger(name)
    handler = logging.StreamHandler()
    formatter = DefaultFormatter('%(levelprefix)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def read_config(
    archive: str,
    keys: Union[str, List[str]],
    config_file: str = 'pyproject.toml',
) -> Dict[str, Any]:
    """
    Read a key from a Toml config file.

    Parameters
    ----------
    archive : str
        Path to an environment file
    keys : str or List[str]
        Period-separated paths to the desired keys
    config_file : str, optional
        Name of the config file in the zip file

    Returns
    -------
    Dict[str, Any]

    """
    defaults = {}
    keys = [keys] if isinstance(keys, str) else list(keys)
    with zipfile.ZipFile(archive) as arc:
        try:
            orig_options = tomllib.loads(arc.read(config_file).decode('utf8'))
            verify_python_version(orig_options)
            for key in keys:
                path = key.split('.')
                options = orig_options
                while path:
                    options = options.get(path.pop(0), {})
                for k, v in options.items():
                    defaults[k.lower().replace('-', '_')] = v
        except KeyError:
            pass
    return defaults


def verify_python_version(options: Dict[str, Any]) -> None:
    """Verify the version of Python matches the pyproject.toml requirement."""
    requires_python = options.get('project', {}).get('requires_python', None)
    if not requires_python:
        return

    m = re.match(r'\s*([<=>])+\s*((?:\d+\.)+\d+)\s*', requires_python)
    if not m:
        raise ValueError(f'python version string is not valid: {requires_python}')

    operator = m.group(1)
    version_info = tuple(int(x) for x in m.group(2))

    if operator == '<=':
        if not (sys.version_info <= version_info):
            raise RuntimeError(
                'python version is not compatible: ' +
                f'{sys.version_info} > {m.group(2)}',
            )

    elif operator == '>=':
        if not (sys.version_info >= version_info):
            raise RuntimeError(
                'python version is not compatible: ' +
                f'{sys.version_info} < {m.group(2)}',
            )

    elif operator in ['==', '=']:
        if not (sys.version_info == version_info):
            raise RuntimeError(
                'python version is not compatible: ' +
                f'{sys.version_info} != {m.group(2)}',
            )

    elif operator == '>':
        if not (sys.version_info > version_info):
            raise RuntimeError(
                'python version is not compatible: ' +
                f'{sys.version_info} <= {m.group(2)}',
            )

    elif operator == '<':
        if not (sys.version_info < version_info):
            raise RuntimeError(
                'python version is not compatible: ' +
                f'{sys.version_info} >= {m.group(2)}',
            )

    else:
        raise ValueError(f'invalid python_version operator: {operator}')


def to_toml(data: Dict[str, Any]) -> str:
    """Dump data to a pyproject.toml."""
    out = []
    for top_k, top_v in data.items():
        if top_v is None:
            continue
        top_k = top_k.replace('_', '-')
        out.append('')
        out.append(f'[{top_k}]')
        for k, v in top_v.items():
            if v is None:
                continue
            k = k.replace('_', '-')
            if isinstance(v, (tuple, list)):
                out.append(f'{k} = [')
                items = []
                for item in v:
                    if item is None:
                        pass
                    elif isinstance(item, (tuple, list)):
                        items.append(f'  {json.dumps(item)}')
                    elif isinstance(item, dict):
                        items.append(
                            re.sub(r'"([^"]+)":', r'\1 =', f'  {json.dumps(item)}'),
                        )
                    else:
                        items.append(f'  {json.dumps([item])[1:-1]}')
                out.append(',\n'.join(items))
                out.append(']')
            elif isinstance(v, dict):
                out.append(re.sub(r'"([^"]+)":', r'\1 =', f'  {json.dumps(v)}'))
            else:
                out.append(f'{k} = {json.dumps([v])[1:-1]}')
    return '\n'.join(out).strip()
