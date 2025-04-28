#!/usr/bin/env python3
"""
Web application for SingleStoreDB external functions.

This module supplies a function that can create web apps intended for use
with the external function feature of SingleStoreDB. The application
function is a standard ASGI <https://asgi.readthedocs.io/en/latest/index.html>
request handler for use with servers such as Uvicorn <https://www.uvicorn.org>.

An external function web application can be created using the `create_app`
function. By default, the exported Python functions are specified by
environment variables starting with SINGLESTOREDB_EXT_FUNCTIONS. See the
documentation in `create_app` for the full syntax. If the application is
created in Python code rather than from the command-line, exported
functions can be specified in the parameters.

An example of starting a server is shown below.

Example
-------
> SINGLESTOREDB_EXT_FUNCTIONS='myfuncs.[percentile_90,percentile_95]' \
    python3 -m singlestoredb.functions.ext.asgi

"""
import argparse
import asyncio
import dataclasses
import importlib.util
import inspect
import io
import itertools
import json
import logging
import os
import re
import secrets
import sys
import tempfile
import textwrap
import typing
import urllib
import zipfile
import zipimport
from types import ModuleType
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple
from typing import Union

from . import arrow
from . import json as jdata
from . import rowdat_1
from . import utils
from ... import connection
from ... import manage_workspaces
from ...config import get_option
from ...mysql.constants import FIELD_TYPE as ft
from ..signature import get_signature
from ..signature import signature_to_sql
from ..typing import Masked
from ..typing import Table

try:
    import cloudpickle
    has_cloudpickle = True
except ImportError:
    has_cloudpickle = False

try:
    from pydantic import BaseModel
    has_pydantic = True
except ImportError:
    has_pydantic = False


logger = utils.get_logger('singlestoredb.functions.ext.asgi')


# If a number of processes is specified, create a pool of workers
num_processes = max(0, int(os.environ.get('SINGLESTOREDB_EXT_NUM_PROCESSES', 0)))
if num_processes > 1:
    try:
        from ray.util.multiprocessing import Pool
    except ImportError:
        from multiprocessing import Pool
    func_map = Pool(num_processes).starmap
else:
    func_map = itertools.starmap


# Use negative values to indicate unsigned ints / binary data / usec time precision
rowdat_1_type_map = {
    'bool': ft.LONGLONG,
    'int8': ft.LONGLONG,
    'int16': ft.LONGLONG,
    'int32': ft.LONGLONG,
    'int64': ft.LONGLONG,
    'uint8': -ft.LONGLONG,
    'uint16': -ft.LONGLONG,
    'uint32': -ft.LONGLONG,
    'uint64': -ft.LONGLONG,
    'float32': ft.DOUBLE,
    'float64': ft.DOUBLE,
    'str': ft.STRING,
    'bytes': -ft.STRING,
}


def get_func_names(funcs: str) -> List[Tuple[str, str]]:
    """
    Parse all function names from string.

    Parameters
    ----------
    func_names : str
        String containing one or more function names. The syntax is
        as follows: [func-name-1@func-alias-1,func-name-2@func-alias-2,...].
        The optional '@name' portion is an alias if you want the function
        to be renamed.

    Returns
    -------
    List[Tuple[str]] : a list of tuples containing the names and aliases
        of each function.

    """
    if funcs.startswith('['):
        func_names = funcs.replace('[', '').replace(']', '').split(',')
        func_names = [x.strip() for x in func_names]
    else:
        func_names = [funcs]

    out = []
    for name in func_names:
        alias = name
        if '@' in name:
            name, alias = name.split('@', 1)
        out.append((name, alias))

    return out


def as_tuple(x: Any) -> Any:
    """Convert object to tuple."""
    if has_pydantic and isinstance(x, BaseModel):
        return tuple(x.model_dump().values())
    if dataclasses.is_dataclass(x):
        return dataclasses.astuple(x)  # type: ignore
    if isinstance(x, dict):
        return tuple(x.values())
    return tuple(x)


def as_list_of_tuples(x: Any) -> Any:
    """Convert object to a list of tuples."""
    if isinstance(x, Table):
        x = x[0]
    if isinstance(x, (list, tuple)) and len(x) > 0:
        if isinstance(x[0], (list, tuple)):
            return x
        if has_pydantic and isinstance(x[0], BaseModel):
            return [tuple(y.model_dump().values()) for y in x]
        if dataclasses.is_dataclass(x[0]):
            return [dataclasses.astuple(y) for y in x]
        if isinstance(x[0], dict):
            return [tuple(y.values()) for y in x]
        return [(y,) for y in x]
    return x


def get_dataframe_columns(df: Any) -> List[Any]:
    """Return columns of data from a dataframe/table."""
    if isinstance(df, Table):
        if len(df) == 1:
            df = df[0]
        else:
            return list(df)

    if isinstance(df, Masked):
        return [df]

    if isinstance(df, tuple):
        return list(df)

    rtype = str(type(df)).lower()
    if 'dataframe' in rtype:
        return [df[x] for x in df.columns]
    elif 'table' in rtype:
        return df.columns
    elif 'series' in rtype:
        return [df]
    elif 'array' in rtype:
        return [df]
    elif 'tuple' in rtype:
        return list(df)

    raise TypeError(
        'Unsupported data type for dataframe columns: '
        f'{rtype}',
    )


def get_array_class(data_format: str) -> Callable[..., Any]:
    """
    Get the array class for the current data format.

    """
    if data_format == 'polars':
        import polars as pl
        array_cls = pl.Series
    elif data_format == 'arrow':
        import pyarrow as pa
        array_cls = pa.array
    elif data_format == 'pandas':
        import pandas as pd
        array_cls = pd.Series
    else:
        import numpy as np
        array_cls = np.array
    return array_cls


def get_masked_params(func: Callable[..., Any]) -> List[bool]:
    """
    Get the list of masked parameters for the function.

    Parameters
    ----------
    func : Callable
        The function to call as the endpoint

    Returns
    -------
    List[bool]
        Boolean list of masked parameters

    """
    params = inspect.signature(func).parameters
    return [typing.get_origin(x.annotation) is Masked for x in params.values()]


def build_tuple(x: Any) -> Any:
    """Convert object to tuple."""
    return tuple(x) if isinstance(x, Masked) else (x, None)


def build_udf_endpoint(
    func: Callable[..., Any],
    returns_data_format: str,
) -> Callable[..., Any]:
    """
    Build a UDF endpoint for scalar / list types (row-based).

    Parameters
    ----------
    func : Callable
        The function to call as the endpoint
    returns_data_format : str
        The format of the return values

    Returns
    -------
    Callable
        The function endpoint

    """
    if returns_data_format in ['scalar', 'list']:

        async def do_func(
            row_ids: Sequence[int],
            rows: Sequence[Sequence[Any]],
        ) -> Tuple[Sequence[int], List[Tuple[Any, ...]]]:
            '''Call function on given rows of data.'''
            return row_ids, [as_tuple(x) for x in zip(func_map(func, rows))]

        return do_func

    return build_vector_udf_endpoint(func, returns_data_format)


def build_vector_udf_endpoint(
    func: Callable[..., Any],
    returns_data_format: str,
) -> Callable[..., Any]:
    """
    Build a UDF endpoint for vector formats (column-based).

    Parameters
    ----------
    func : Callable
        The function to call as the endpoint
    returns_data_format : str
        The format of the return values

    Returns
    -------
    Callable
        The function endpoint

    """
    masks = get_masked_params(func)
    array_cls = get_array_class(returns_data_format)

    async def do_func(
        row_ids: Sequence[int],
        cols: Sequence[Tuple[Sequence[Any], Optional[Sequence[bool]]]],
    ) -> Tuple[
        Sequence[int],
        List[Tuple[Sequence[Any], Optional[Sequence[bool]]]],
    ]:
        '''Call function on given columns of data.'''
        row_ids = array_cls(row_ids)

        # Call the function with `cols` as the function parameters
        if cols and cols[0]:
            out = func(*[x if m else x[0] for x, m in zip(cols, masks)])
        else:
            out = func()

        # Single masked value
        if isinstance(out, Masked):
            return row_ids, [tuple(out)]

        # Multiple return values
        if isinstance(out, tuple):
            return row_ids, [build_tuple(x) for x in out]

        # Single return value
        return row_ids, [(out, None)]

    return do_func


def build_tvf_endpoint(
    func: Callable[..., Any],
    returns_data_format: str,
) -> Callable[..., Any]:
    """
    Build a TVF endpoint for scalar / list types (row-based).

    Parameters
    ----------
    func : Callable
        The function to call as the endpoint
    returns_data_format : str
        The format of the return values

    Returns
    -------
    Callable
        The function endpoint

    """
    if returns_data_format in ['scalar', 'list']:

        async def do_func(
            row_ids: Sequence[int],
            rows: Sequence[Sequence[Any]],
        ) -> Tuple[Sequence[int], List[Tuple[Any, ...]]]:
            '''Call function on given rows of data.'''
            out_ids: List[int] = []
            out = []
            # Call function on each row of data
            for i, res in zip(row_ids, func_map(func, rows)):
                out.extend(as_list_of_tuples(res))
                out_ids.extend([row_ids[i]] * (len(out)-len(out_ids)))
            return out_ids, out

        return do_func

    return build_vector_tvf_endpoint(func, returns_data_format)


def build_vector_tvf_endpoint(
    func: Callable[..., Any],
    returns_data_format: str,
) -> Callable[..., Any]:
    """
    Build a TVF endpoint for vector formats (column-based).

    Parameters
    ----------
    func : Callable
        The function to call as the endpoint
    returns_data_format : str
        The format of the return values

    Returns
    -------
    Callable
        The function endpoint

    """
    masks = get_masked_params(func)
    array_cls = get_array_class(returns_data_format)

    async def do_func(
        row_ids: Sequence[int],
        cols: Sequence[Tuple[Sequence[Any], Optional[Sequence[bool]]]],
    ) -> Tuple[
        Sequence[int],
        List[Tuple[Sequence[Any], Optional[Sequence[bool]]]],
    ]:
        '''Call function on given columns of data.'''
        # NOTE: There is no way to determine which row ID belongs to
        #        each result row, so we just have to use the same
        #        row ID for all rows in the result.

        # Call function on each column of data
        if cols and cols[0]:
            res = get_dataframe_columns(
                func(*[x if m else x[0] for x, m in zip(cols, masks)]),
            )
        else:
            res = get_dataframe_columns(func())

        # Generate row IDs
        if isinstance(res[0], Masked):
            row_ids = array_cls([row_ids[0]] * len(res[0][0]))
        else:
            row_ids = array_cls([row_ids[0]] * len(res[0]))

        return row_ids, [build_tuple(x) for x in res]

    return do_func


def make_func(
    name: str,
    func: Callable[..., Any],
) -> Tuple[Callable[..., Any], Dict[str, Any]]:
    """
    Make a function endpoint.

    Parameters
    ----------
    name : str
        Name of the function to create
    func : Callable
        The function to call as the endpoint

    Returns
    -------
    (Callable, Dict[str, Any])

    """
    info: Dict[str, Any] = {}

    sig = get_signature(func, func_name=name)

    function_type = sig.get('function_type', 'udf')
    args_data_format = sig.get('args_data_format', 'scalar')
    returns_data_format = sig.get('returns_data_format', 'scalar')

    if function_type == 'tvf':
        do_func = build_tvf_endpoint(func, returns_data_format)
    else:
        do_func = build_udf_endpoint(func, returns_data_format)

    do_func.__name__ = name
    do_func.__doc__ = func.__doc__

    # Store signature for generating CREATE FUNCTION calls
    info['signature'] = sig

    # Set data format
    info['args_data_format'] = args_data_format
    info['returns_data_format'] = returns_data_format

    # Set function type
    info['function_type'] = function_type

    # Setup argument types for rowdat_1 parser
    colspec = []
    for x in sig['args']:
        dtype = x['dtype'].replace('?', '')
        if dtype not in rowdat_1_type_map:
            raise TypeError(f'no data type mapping for {dtype}')
        colspec.append((x['name'], rowdat_1_type_map[dtype]))
    info['colspec'] = colspec

    # Setup return type
    returns = []
    for x in sig['returns']:
        dtype = x['dtype'].replace('?', '')
        if dtype not in rowdat_1_type_map:
            raise TypeError(f'no data type mapping for {dtype}')
        returns.append((x['name'], rowdat_1_type_map[dtype]))
    info['returns'] = returns

    return do_func, info


class Application(object):
    """
    Create an external function application.

    If `functions` is None, the environment is searched for function
    specifications in variables starting with `SINGLESTOREDB_EXT_FUNCTIONS`.
    Any number of environment variables can be specified as long as they
    have this prefix. The format of the environment variable value is the
    same as for the `functions` parameter.

    Parameters
    ----------
    functions : str or Iterable[str], optional
        Python functions are specified using a string format as follows:
            * Single function : <pkg1>.<func1>
            * Multiple functions : <pkg1>.[<func1-name,func2-name,...]
            * Function aliases : <pkg1>.[<func1@alias1,func2@alias2,...]
            * Multiple packages : <pkg1>.<func1>:<pkg2>.<func2>
    app_mode : str, optional
        The mode of operation for the application: remote, managed, or collocated
    url : str, optional
        The URL of the function API
    data_format : str, optional
        The format of the data rows: 'rowdat_1' or 'json'
    data_version : str, optional
        The version of the call format to expect: '1.0'
    link_name : str, optional
        The link name to use for the external function application. This is
        only for pre-existing links, and can only be used without
        ``link_config`` and ``link_credentials``.
    link_config : Dict[str, Any], optional
        The CONFIG section of a LINK definition. This dictionary gets
        converted to JSON for the CREATE LINK call.
    link_credentials : Dict[str, Any], optional
        The CREDENTIALS section of a LINK definition. This dictionary gets
        converted to JSON for the CREATE LINK call.

    """

    # Plain text response start
    text_response_dict: Dict[str, Any] = dict(
        type='http.response.start',
        status=200,
        headers=[(b'content-type', b'text/plain')],
    )

    # Error response start
    error_response_dict: Dict[str, Any] = dict(
        type='http.response.start',
        status=401,
        headers=[(b'content-type', b'text/plain')],
    )

    # JSON response start
    json_response_dict: Dict[str, Any] = dict(
        type='http.response.start',
        status=200,
        headers=[(b'content-type', b'application/json')],
    )

    # ROWDAT_1 response start
    rowdat_1_response_dict: Dict[str, Any] = dict(
        type='http.response.start',
        status=200,
        headers=[(b'content-type', b'x-application/rowdat_1')],
    )

    # Apache Arrow response start
    arrow_response_dict: Dict[str, Any] = dict(
        type='http.response.start',
        status=200,
        headers=[(b'content-type', b'application/vnd.apache.arrow.file')],
    )

    # Path not found response start
    path_not_found_response_dict: Dict[str, Any] = dict(
        type='http.response.start',
        status=404,
    )

    # Response body template
    body_response_dict: Dict[str, Any] = dict(
        type='http.response.body',
    )

    # Data format + version handlers
    handlers = {
        (b'application/octet-stream', b'1.0', 'scalar'): dict(
            load=rowdat_1.load,
            dump=rowdat_1.dump,
            response=rowdat_1_response_dict,
        ),
        (b'application/octet-stream', b'1.0', 'list'): dict(
            load=rowdat_1.load,
            dump=rowdat_1.dump,
            response=rowdat_1_response_dict,
        ),
        (b'application/octet-stream', b'1.0', 'pandas'): dict(
            load=rowdat_1.load_pandas,
            dump=rowdat_1.dump_pandas,
            response=rowdat_1_response_dict,
        ),
        (b'application/octet-stream', b'1.0', 'numpy'): dict(
            load=rowdat_1.load_numpy,
            dump=rowdat_1.dump_numpy,
            response=rowdat_1_response_dict,
        ),
        (b'application/octet-stream', b'1.0', 'polars'): dict(
            load=rowdat_1.load_polars,
            dump=rowdat_1.dump_polars,
            response=rowdat_1_response_dict,
        ),
        (b'application/octet-stream', b'1.0', 'arrow'): dict(
            load=rowdat_1.load_arrow,
            dump=rowdat_1.dump_arrow,
            response=rowdat_1_response_dict,
        ),
        (b'application/json', b'1.0', 'scalar'): dict(
            load=jdata.load,
            dump=jdata.dump,
            response=json_response_dict,
        ),
        (b'application/json', b'1.0', 'list'): dict(
            load=jdata.load,
            dump=jdata.dump,
            response=json_response_dict,
        ),
        (b'application/json', b'1.0', 'pandas'): dict(
            load=jdata.load_pandas,
            dump=jdata.dump_pandas,
            response=json_response_dict,
        ),
        (b'application/json', b'1.0', 'numpy'): dict(
            load=jdata.load_numpy,
            dump=jdata.dump_numpy,
            response=json_response_dict,
        ),
        (b'application/json', b'1.0', 'polars'): dict(
            load=jdata.load_polars,
            dump=jdata.dump_polars,
            response=json_response_dict,
        ),
        (b'application/json', b'1.0', 'arrow'): dict(
            load=jdata.load_arrow,
            dump=jdata.dump_arrow,
            response=json_response_dict,
        ),
        (b'application/vnd.apache.arrow.file', b'1.0', 'scalar'): dict(
            load=arrow.load,
            dump=arrow.dump,
            response=arrow_response_dict,
        ),
        (b'application/vnd.apache.arrow.file', b'1.0', 'pandas'): dict(
            load=arrow.load_pandas,
            dump=arrow.dump_pandas,
            response=arrow_response_dict,
        ),
        (b'application/vnd.apache.arrow.file', b'1.0', 'numpy'): dict(
            load=arrow.load_numpy,
            dump=arrow.dump_numpy,
            response=arrow_response_dict,
        ),
        (b'application/vnd.apache.arrow.file', b'1.0', 'polars'): dict(
            load=arrow.load_polars,
            dump=arrow.dump_polars,
            response=arrow_response_dict,
        ),
        (b'application/vnd.apache.arrow.file', b'1.0', 'arrow'): dict(
            load=arrow.load_arrow,
            dump=arrow.dump_arrow,
            response=arrow_response_dict,
        ),
    }

    # Valid URL paths
    invoke_path = ('invoke',)
    show_create_function_path = ('show', 'create_function')
    show_function_info_path = ('show', 'function_info')

    def __init__(
        self,
        functions: Optional[
            Union[
                str,
                Iterable[str],
                Callable[..., Any],
                Iterable[Callable[..., Any]],
                ModuleType,
                Iterable[ModuleType],
            ]
        ] = None,
        app_mode: str = get_option('external_function.app_mode'),
        url: str = get_option('external_function.url'),
        data_format: str = get_option('external_function.data_format'),
        data_version: str = get_option('external_function.data_version'),
        link_name: Optional[str] = get_option('external_function.link_name'),
        link_config: Optional[Dict[str, Any]] = None,
        link_credentials: Optional[Dict[str, Any]] = None,
        name_prefix: str = get_option('external_function.name_prefix'),
        name_suffix: str = get_option('external_function.name_suffix'),
    ) -> None:
        if link_name and (link_config or link_credentials):
            raise ValueError(
                '`link_name` can not be used with `link_config` or `link_credentials`',
            )

        if link_config is None:
            link_config = json.loads(
                get_option('external_function.link_config') or '{}',
            ) or None

        if link_credentials is None:
            link_credentials = json.loads(
                get_option('external_function.link_credentials') or '{}',
            ) or None

        # List of functions specs
        specs: List[Union[str, Callable[..., Any], ModuleType]] = []

        # Look up Python function specifications
        if functions is None:
            env_vars = [
                x for x in os.environ.keys()
                if x.startswith('SINGLESTOREDB_EXT_FUNCTIONS')
            ]
            if env_vars:
                specs = [os.environ[x] for x in env_vars]
            else:
                import __main__
                specs = [__main__]

        elif isinstance(functions, ModuleType):
            specs = [functions]

        elif isinstance(functions, str):
            specs = [functions]

        elif callable(functions):
            specs = [functions]

        else:
            specs = list(functions)

        # Add functions to application
        endpoints = dict()
        external_functions = dict()
        for funcs in itertools.chain(specs):

            if isinstance(funcs, str):
                # Module name
                if importlib.util.find_spec(funcs) is not None:
                    items = importlib.import_module(funcs)
                    for x in vars(items).values():
                        if not hasattr(x, '_singlestoredb_attrs'):
                            continue
                        name = x._singlestoredb_attrs.get('name', x.__name__)
                        name = f'{name_prefix}{name}{name_suffix}'
                        external_functions[x.__name__] = x
                        func, info = make_func(name, x)
                        endpoints[name.encode('utf-8')] = func, info

                # Fully qualified function name
                elif '.' in funcs:
                    pkg_path, func_names = funcs.rsplit('.', 1)
                    pkg = importlib.import_module(pkg_path)

                    if pkg is None:
                        raise RuntimeError(f'Could not locate module: {pkg}')

                    # Add endpoint for each exported function
                    for name, alias in get_func_names(func_names):
                        item = getattr(pkg, name)
                        alias = f'{name_prefix}{name}{name_suffix}'
                        external_functions[name] = item
                        func, info = make_func(alias, item)
                        endpoints[alias.encode('utf-8')] = func, info

                else:
                    raise RuntimeError(f'Could not locate module: {funcs}')

            elif isinstance(funcs, ModuleType):
                for x in vars(funcs).values():
                    if not hasattr(x, '_singlestoredb_attrs'):
                        continue
                    name = x._singlestoredb_attrs.get('name', x.__name__)
                    name = f'{name_prefix}{name}{name_suffix}'
                    external_functions[x.__name__] = x
                    func, info = make_func(name, x)
                    endpoints[name.encode('utf-8')] = func, info

            else:
                alias = funcs.__name__
                external_functions[funcs.__name__] = funcs
                alias = f'{name_prefix}{alias}{name_suffix}'
                func, info = make_func(alias, funcs)
                endpoints[alias.encode('utf-8')] = func, info

        self.app_mode = app_mode
        self.url = url
        self.data_format = data_format
        self.data_version = data_version
        self.link_name = link_name
        self.link_config = link_config
        self.link_credentials = link_credentials
        self.endpoints = endpoints
        self.external_functions = external_functions

    async def __call__(
        self,
        scope: Dict[str, Any],
        receive: Callable[..., Awaitable[Any]],
        send: Callable[..., Awaitable[Any]],
    ) -> None:
        '''
        Application request handler.

        Parameters
        ----------
        scope : dict
            ASGI request scope
        receive : Callable
            Function to receieve request information
        send : Callable
            Function to send response information

        '''
        assert scope['type'] == 'http'

        method = scope['method']
        path = tuple(x for x in scope['path'].split('/') if x)
        headers = dict(scope['headers'])

        content_type = headers.get(
            b'content-type',
            b'application/octet-stream',
        )
        accepts = headers.get(b'accepts', content_type)
        func_name = headers.get(b's2-ef-name', b'')
        func_endpoint = self.endpoints.get(func_name)

        func = None
        func_info: Dict[str, Any] = {}
        if func_endpoint is not None:
            func, func_info = func_endpoint

        # Call the endpoint
        if method == 'POST' and func is not None and path == self.invoke_path:
            args_data_format = func_info['args_data_format']
            returns_data_format = func_info['returns_data_format']
            data = []
            more_body = True
            while more_body:
                request = await receive()
                data.append(request['body'])
                more_body = request.get('more_body', False)

            data_version = headers.get(b's2-ef-version', b'')
            input_handler = self.handlers[(content_type, data_version, args_data_format)]
            output_handler = self.handlers[(accepts, data_version, returns_data_format)]

            try:
                out = await func(
                    *input_handler['load'](  # type: ignore
                        func_info['colspec'], b''.join(data),
                    ),
                )
                body = output_handler['dump'](
                    [x[1] for x in func_info['returns']], *out,  # type: ignore
                )
                await send(output_handler['response'])

            except Exception as e:
                logging.exception('Error in function call')
                body = f'[{type(e).__name__}] {str(e).strip()}'.encode('utf-8')
                await send(self.error_response_dict)

        # Handle api reflection
        elif method == 'GET' and path == self.show_create_function_path:
            host = headers.get(b'host', b'localhost:80')
            reflected_url = f'{scope["scheme"]}://{host.decode("utf-8")}/invoke'

            syntax = []
            for key, (endpoint, endpoint_info) in self.endpoints.items():
                if not func_name or key == func_name:
                    syntax.append(
                        signature_to_sql(
                            endpoint_info['signature'],
                            url=self.url or reflected_url,
                            data_format=self.data_format,
                        ),
                    )
            body = '\n'.join(syntax).encode('utf-8')

            await send(self.text_response_dict)

        # Return function info
        elif method == 'GET' and (path == self.show_function_info_path or not path):
            functions = self.get_function_info()
            body = json.dumps(dict(functions=functions)).encode('utf-8')
            await send(self.text_response_dict)

        # Path not found
        else:
            body = b''
            await send(self.path_not_found_response_dict)

        # Send body
        out = self.body_response_dict.copy()
        out['body'] = body
        await send(out)

    def _create_link(
        self,
        config: Optional[Dict[str, Any]],
        credentials: Optional[Dict[str, Any]],
    ) -> Tuple[str, str]:
        """Generate CREATE LINK command."""
        if self.link_name:
            return self.link_name, ''

        if not config and not credentials:
            return '', ''

        link_name = f'py_ext_func_link_{secrets.token_hex(14)}'
        out = [f'CREATE LINK {link_name} AS HTTP']

        if config:
            out.append(f"CONFIG '{json.dumps(config)}'")

        if credentials:
            out.append(f"CREDENTIALS '{json.dumps(credentials)}'")

        return link_name, ' '.join(out) + ';'

    def _locate_app_functions(self, cur: Any) -> Tuple[Set[str], Set[str]]:
        """Locate all current functions and links belonging to this app."""
        funcs, links = set(), set()
        cur.execute('SHOW FUNCTIONS')
        for row in list(cur):
            name, ftype, link = row[0], row[1], row[-1]
            # Only look at external functions
            if 'external' not in ftype.lower():
                continue
            # See if function URL matches url
            cur.execute(f'SHOW CREATE FUNCTION `{name}`')
            for fname, _, code, *_ in list(cur):
                m = re.search(r" (?:\w+) (?:SERVICE|MANAGED) '([^']+)'", code)
                if m and m.group(1) == self.url:
                    funcs.add(fname)
                    if link and re.match(r'^py_ext_func_link_\S{14}$', link):
                        links.add(link)
        return funcs, links

    def get_function_info(
        self,
        func_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Return the functions and function signature information.
        Returns
        -------
        Dict[str, Any]
        """
        functions = {}
        no_default = object()

        for key, (_, info) in self.endpoints.items():
            if not func_name or key == func_name:
                sig = info['signature']
                args = []

                # Function arguments
                for a in sig.get('args', []):
                    dtype = a['dtype']
                    nullable = '?' in dtype
                    args.append(
                        dict(
                            name=a['name'],
                            dtype=dtype.replace('?', ''),
                            nullable=nullable,
                        ),
                    )
                    if a.get('default', no_default) is not no_default:
                        args[-1]['default'] = a['default']

                # Return values
                ret = sig.get('returns', [])
                returns = []

                for a in ret:
                    dtype = a['dtype']
                    nullable = '?' in dtype
                    returns.append(
                        dict(
                            dtype=dtype.replace('?', ''),
                            nullable=nullable,
                        ),
                    )
                    if a.get('name', None):
                        returns[-1]['name'] = a['name']
                    if a.get('default', no_default) is not no_default:
                        returns[-1]['default'] = a['default']

                functions[sig['name']] = dict(
                    args=args, returns=returns, function_type=info['function_type'],
                )

        return functions

    def get_create_functions(
        self,
        replace: bool = False,
    ) -> List[str]:
        """
        Generate CREATE FUNCTION code for all functions.

        Parameters
        ----------
        replace : bool, optional
            Should existing functions be replaced?

        Returns
        -------
        List[str]

        """
        if not self.endpoints:
            return []

        out = []
        link = ''
        if self.app_mode.lower() == 'remote':
            link, link_str = self._create_link(self.link_config, self.link_credentials)
            if link and link_str:
                out.append(link_str)

        for key, (endpoint, endpoint_info) in self.endpoints.items():
            out.append(
                signature_to_sql(
                    endpoint_info['signature'],
                    url=self.url,
                    data_format=self.data_format,
                    app_mode=self.app_mode,
                    replace=replace,
                    link=link or None,
                ),
            )

        return out

    def register_functions(
        self,
        *connection_args: Any,
        replace: bool = False,
        **connection_kwargs: Any,
    ) -> None:
        """
        Register functions with the database.

        Parameters
        ----------
        *connection_args : Any
            Database connection parameters
        replace : bool, optional
            Should existing functions be replaced?
        **connection_kwargs : Any
            Database connection parameters

        """
        with connection.connect(*connection_args, **connection_kwargs) as conn:
            with conn.cursor() as cur:
                if replace:
                    funcs, links = self._locate_app_functions(cur)
                    for fname in funcs:
                        cur.execute(f'DROP FUNCTION IF EXISTS `{fname}`')
                    for link in links:
                        cur.execute(f'DROP LINK {link}')
                for func in self.get_create_functions(replace=replace):
                    cur.execute(func)

    def drop_functions(
        self,
        *connection_args: Any,
        **connection_kwargs: Any,
    ) -> None:
        """
        Drop registered functions from database.

        Parameters
        ----------
        *connection_args : Any
            Database connection parameters
        **connection_kwargs : Any
            Database connection parameters

        """
        with connection.connect(*connection_args, **connection_kwargs) as conn:
            with conn.cursor() as cur:
                funcs, links = self._locate_app_functions(cur)
                for fname in funcs:
                    cur.execute(f'DROP FUNCTION IF EXISTS `{fname}`')
                for link in links:
                    cur.execute(f'DROP LINK {link}')

    async def call(
        self,
        name: str,
        data_in: io.BytesIO,
        data_out: io.BytesIO,
        data_format: Optional[str] = None,
        data_version: Optional[str] = None,
    ) -> None:
        """
        Call a function in the application.

        Parameters
        ----------
        name : str
            Name of the function to call
        data_in : io.BytesIO
            The input data rows
        data_out : io.BytesIO
            The output data rows
        data_format : str, optional
            The format of the input and output data
        data_version : str, optional
            The version of the data format

        """
        data_format = data_format or self.data_format
        data_version = data_version or self.data_version

        async def receive() -> Dict[str, Any]:
            return dict(body=data_in.read())

        async def send(content: Dict[str, Any]) -> None:
            status = content.get('status', 200)
            if status != 200:
                raise KeyError(f'error occurred when calling `{name}`: {status}')
            data_out.write(content.get('body', b''))

        accepts = dict(
            json=b'application/json',
            rowdat_1=b'application/octet-stream',
            arrow=b'application/vnd.apache.arrow.file',
        )

        # Mock an ASGI scope
        scope = dict(
            type='http',
            path='invoke',
            method='POST',
            headers={
                b'content-type': accepts[data_format.lower()],
                b'accepts': accepts[data_format.lower()],
                b's2-ef-name': name.encode('utf-8'),
                b's2-ef-version': data_version.encode('utf-8'),
            },
        )

        await self(scope, receive, send)

    def to_environment(
        self,
        name: str,
        destination: str = '.',
        version: Optional[str] = None,
        dependencies: Optional[List[str]] = None,
        authors: Optional[List[Dict[str, str]]] = None,
        maintainers: Optional[List[Dict[str, str]]] = None,
        description: Optional[str] = None,
        container_service: Optional[Dict[str, Any]] = None,
        external_function: Optional[Dict[str, Any]] = None,
        external_function_remote: Optional[Dict[str, Any]] = None,
        external_function_collocated: Optional[Dict[str, Any]] = None,
        overwrite: bool = False,
    ) -> None:
        """
        Convert application to an environment file.

        Parameters
        ----------
        name : str
            Name of the output environment
        destination : str, optional
            Location of the output file
        version : str, optional
            Version of the package
        dependencies : List[str], optional
            List of dependency specifications like in a requirements.txt file
        authors : List[Dict[str, Any]], optional
            Dictionaries of author information. Keys may include: email, name
        maintainers : List[Dict[str, Any]], optional
            Dictionaries of maintainer information. Keys may include: email, name
        description : str, optional
            Description of package
        container_service : Dict[str, Any], optional
            Container service specifications
        external_function : Dict[str, Any], optional
            External function specifications (applies to both remote and collocated)
        external_function_remote : Dict[str, Any], optional
            Remote external function specifications
        external_function_collocated : Dict[str, Any], optional
            Collocated external function specifications
        overwrite : bool, optional
            Should destination file be overwritten if it exists?

        """
        if not has_cloudpickle:
            raise RuntimeError('the cloudpicke package is required for this operation')

        # Write to temporary location if a remote destination is specified
        tmpdir = None
        if destination.startswith('stage://'):
            tmpdir = tempfile.TemporaryDirectory()
            local_path = os.path.join(tmpdir.name, f'{name}.env')
        else:
            local_path = os.path.join(destination, f'{name}.env')
            if not overwrite and os.path.exists(local_path):
                raise OSError(f'path already exists: {local_path}')

        with zipfile.ZipFile(local_path, mode='w') as z:
            # Write metadata
            z.writestr(
                'pyproject.toml', utils.to_toml({
                    'project': dict(
                        name=name,
                        version=version,
                        dependencies=dependencies,
                        requires_python='== ' +
                        '.'.join(str(x) for x in sys.version_info[:3]),
                        authors=authors,
                        maintainers=maintainers,
                        description=description,
                    ),
                    'tool.container-service': container_service,
                    'tool.external-function': external_function,
                    'tool.external-function.remote': external_function_remote,
                    'tool.external-function.collocated': external_function_collocated,
                }),
            )

            # Write Python package
            z.writestr(
                f'{name}/__init__.py',
                textwrap.dedent(f'''
                    import pickle as _pkl
                    globals().update(
                        _pkl.loads({cloudpickle.dumps(self.external_functions)}),
                    )
                    __all__ = {list(self.external_functions.keys())}''').strip(),
            )

        # Upload to Stage as needed
        if destination.startswith('stage://'):
            url = urllib.parse.urlparse(re.sub(r'/+$', r'', destination) + '/')
            if not url.path or url.path == '/':
                raise ValueError(f'no stage path was specified: {destination}')

            mgr = manage_workspaces()
            if url.hostname:
                wsg = mgr.get_workspace_group(url.hostname)
            elif os.environ.get('SINGLESTOREDB_WORKSPACE_GROUP'):
                wsg = mgr.get_workspace_group(
                    os.environ['SINGLESTOREDB_WORKSPACE_GROUP'],
                )
            else:
                raise ValueError(f'no workspace group specified: {destination}')

            # Make intermediate directories
            if url.path.count('/') > 1:
                wsg.stage.mkdirs(os.path.dirname(url.path))

            wsg.stage.upload_file(
                local_path, url.path + f'{name}.env',
                overwrite=overwrite,
            )
            os.remove(local_path)


def main(argv: Optional[List[str]] = None) -> None:
    """
    Main program for HTTP-based Python UDFs

    Parameters
    ----------
    argv : List[str], optional
        List of command-line parameters

    """
    try:
        import uvicorn
    except ImportError:
        raise ImportError('the uvicorn package is required to run this command')

    # Should we run in embedded mode (typically for Jupyter)
    try:
        asyncio.get_running_loop()
        use_async = True
    except RuntimeError:
        use_async = False

    # Temporary directory for Stage environment files
    tmpdir = None

    # Depending on whether we find an environment file specified, we
    # may have to process the command line twice.
    functions = []
    defaults: Dict[str, Any] = {}
    for i in range(2):

        parser = argparse.ArgumentParser(
            prog='python -m singlestoredb.functions.ext.asgi',
            description='Run an HTTP-based Python UDF server',
        )
        parser.add_argument(
            '--url', metavar='url',
            default=defaults.get(
                'url',
                get_option('external_function.url'),
            ),
            help='URL of the UDF server endpoint',
        )
        parser.add_argument(
            '--host', metavar='host',
            default=defaults.get(
                'host',
                get_option('external_function.host'),
            ),
            help='bind socket to this host',
        )
        parser.add_argument(
            '--port', metavar='port', type=int,
            default=defaults.get(
                'port',
                get_option('external_function.port'),
            ),
            help='bind socket to this port',
        )
        parser.add_argument(
            '--db', metavar='conn-str',
            default=defaults.get(
                'connection',
                get_option('external_function.connection'),
            ),
            help='connection string to use for registering functions',
        )
        parser.add_argument(
            '--replace-existing', action='store_true',
            help='should existing functions of the same name '
                 'in the database be replaced?',
        )
        parser.add_argument(
            '--data-format', metavar='format',
            default=defaults.get(
                'data_format',
                get_option('external_function.data_format'),
            ),
            choices=['rowdat_1', 'json'],
            help='format of the data rows',
        )
        parser.add_argument(
            '--data-version', metavar='version',
            default=defaults.get(
                'data_version',
                get_option('external_function.data_version'),
            ),
            help='version of the data row format',
        )
        parser.add_argument(
            '--link-name', metavar='name',
            default=defaults.get(
                'link_name',
                get_option('external_function.link_name'),
            ) or '',
            help='name of the link to use for connections',
        )
        parser.add_argument(
            '--link-config', metavar='json',
            default=str(
                defaults.get(
                    'link_config',
                    get_option('external_function.link_config'),
                ) or '{}',
            ),
            help='link config in JSON format',
        )
        parser.add_argument(
            '--link-credentials', metavar='json',
            default=str(
                defaults.get(
                    'link_credentials',
                    get_option('external_function.link_credentials'),
                ) or '{}',
            ),
            help='link credentials in JSON format',
        )
        parser.add_argument(
            '--log-level', metavar='[info|debug|warning|error]',
            default=defaults.get(
                'log_level',
                get_option('external_function.log_level'),
            ),
            help='logging level',
        )
        parser.add_argument(
            '--name-prefix', metavar='name_prefix',
            default=defaults.get(
                'name_prefix',
                get_option('external_function.name_prefix'),
            ),
            help='Prefix to add to function names',
        )
        parser.add_argument(
            '--name-suffix', metavar='name_suffix',
            default=defaults.get(
                'name_suffix',
                get_option('external_function.name_suffix'),
            ),
            help='Suffix to add to function names',
        )
        parser.add_argument(
            'functions', metavar='module.or.func.path', nargs='*',
            help='functions or modules to export in UDF server',
        )

        args = parser.parse_args(argv)

        logger.setLevel(getattr(logging, args.log_level.upper()))

        if i > 0:
            break

        # Download Stage files as needed
        for i, f in enumerate(args.functions):
            if f.startswith('stage://'):
                url = urllib.parse.urlparse(f)
                if not url.path or url.path == '/':
                    raise ValueError(f'no stage path was specified: {f}')
                if url.path.endswith('/'):
                    raise ValueError(f'an environment file must be specified: {f}')

                mgr = manage_workspaces()
                if url.hostname:
                    wsg = mgr.get_workspace_group(url.hostname)
                elif os.environ.get('SINGLESTOREDB_WORKSPACE_GROUP'):
                    wsg = mgr.get_workspace_group(
                        os.environ['SINGLESTOREDB_WORKSPACE_GROUP'],
                    )
                else:
                    raise ValueError(f'no workspace group specified: {f}')

                if tmpdir is None:
                    tmpdir = tempfile.TemporaryDirectory()

                local_path = os.path.join(tmpdir.name, url.path.split('/')[-1])
                wsg.stage.download_file(url.path, local_path)
                args.functions[i] = local_path

            elif f.startswith('http://') or f.startswith('https://'):
                if tmpdir is None:
                    tmpdir = tempfile.TemporaryDirectory()

                local_path = os.path.join(tmpdir.name, f.split('/')[-1])
                urllib.request.urlretrieve(f, local_path)
                args.functions[i] = local_path

        # See if any of the args are zip files (assume they are environment files)
        modules = [(x, zipfile.is_zipfile(x)) for x in args.functions]
        envs = [x[0] for x in modules if x[1]]
        others = [x[0] for x in modules if not x[1]]

        if envs and len(envs) > 1:
            raise RuntimeError('only one environment file may be specified')

        if envs and others:
            raise RuntimeError('environment files and other modules can not be mixed.')

        # See if an environment file was specified. If so, use those settings
        # as the defaults and reprocess command line.
        if envs:
            # Add pyproject.toml variables and redo command-line processing
            defaults = utils.read_config(
                envs[0],
                ['tool.external-function', 'tool.external-function.remote'],
            )

            # Load zip file as a module
            modname = os.path.splitext(os.path.basename(envs[0]))[0]
            zi = zipimport.zipimporter(envs[0])
            mod = zi.load_module(modname)
            if mod is None:
                raise RuntimeError(f'environment file could not be imported: {envs[0]}')
            functions = [mod]

            if defaults:
                continue

    args.functions = functions or args.functions or None
    args.replace_existing = args.replace_existing \
        or defaults.get('replace_existing') \
        or get_option('external_function.replace_existing')

    # Substitute in host / port if specified
    if args.host != defaults.get('host') or args.port != defaults.get('port'):
        u = urllib.parse.urlparse(args.url)
        args.url = u._replace(netloc=f'{args.host}:{args.port}').geturl()

    # Create application from functions / module
    app = Application(
        functions=args.functions,
        url=args.url,
        data_format=args.data_format,
        data_version=args.data_version,
        link_name=args.link_name or None,
        link_config=json.loads(args.link_config) or None,
        link_credentials=json.loads(args.link_credentials) or None,
        app_mode='remote',
        name_prefix=args.name_prefix,
        name_suffix=args.name_suffix,
    )

    funcs = app.get_create_functions(replace=args.replace_existing)
    if not funcs:
        raise RuntimeError('no functions specified')

    for f in funcs:
        logger.info(f)

    try:
        if args.db:
            logger.info('registering functions with database')
            app.register_functions(
                args.db,
                replace=args.replace_existing,
            )

        app_args = {
            k: v for k, v in dict(
                host=args.host or None,
                port=args.port or None,
                log_level=args.log_level,
                lifespan='off',
            ).items() if v is not None
        }

        if use_async:
            asyncio.create_task(_run_uvicorn(uvicorn, app, app_args, db=args.db))
        else:
            uvicorn.run(app, **app_args)

    finally:
        if not use_async and args.db:
            logger.info('dropping functions from database')
            app.drop_functions(args.db)


async def _run_uvicorn(
    uvicorn: Any,
    app: Any,
    app_args: Any,
    db: Optional[str] = None,
) -> None:
    """Run uvicorn server and clean up functions after shutdown."""
    await uvicorn.Server(uvicorn.Config(app, **app_args)).serve()
    if db:
        logger.info('dropping functions from database')
        app.drop_functions(db)


create_app = Application


if __name__ == '__main__':
    try:
        main()
    except RuntimeError as exc:
        logger.error(str(exc))
        sys.exit(1)
    except KeyboardInterrupt:
        pass
