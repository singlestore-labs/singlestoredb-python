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
import contextvars
import dataclasses
import datetime
import functools
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
import threading
import time
import traceback
import typing
import urllib
import uuid
import zipfile
import zipimport
from collections.abc import Awaitable
from collections.abc import Iterable
from collections.abc import Sequence
from threading import Event
from types import ModuleType
from typing import Any
from typing import Callable
from typing import Coroutine
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Union

try:
    from typing import TypeAlias  # type: ignore
except ImportError:
    from typing_extensions import TypeAlias  # type: ignore

from . import arrow
from . import json as jdata
from . import rowdat_1
from . import utils
from ... import connection
from ... import manage_workspaces
from ...config import get_option
from ...mysql.constants import FIELD_TYPE as ft
from ...docstring.parser import parse
from ..signature import get_signature
from ..signature import signature_to_sql
from ..sql_types import escape_name
from ..typing import Masked
from ..typing import Table
from .timer import Timer

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


async def to_thread(
    func: Any, /, *args: Any, **kwargs: Dict[str, Any],
) -> Any:
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)
    return await loop.run_in_executor(None, func_call)


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
    'json': ft.JSON,
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


def extend_rows(rows: List[Any], x: Any) -> int:
    """Extend list of rows with data from object."""
    if isinstance(x, Table):
        x = x[0]

    if isinstance(x, (list, tuple)) and len(x) > 0:

        if isinstance(x[0], (list, tuple)):
            rows.extend(x)

        elif has_pydantic and isinstance(x[0], BaseModel):
            for y in x:
                rows.append(tuple(y.model_dump().values()))

        elif dataclasses.is_dataclass(x[0]):
            for y in x:
                rows.append(dataclasses.astuple(y))

        elif isinstance(x[0], dict):
            for y in x:
                rows.append(tuple(y.values()))

        else:
            for y in x:
                rows.append((y,))

        return len(x)

    rows.append((x,))

    return 1


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

    # Pandas or polars type of dataframe
    if 'dataframe' in rtype:
        return [df[x] for x in df.columns]
    # PyArrow table
    elif 'table' in rtype:
        return df.columns
    # Pandas or polars series
    elif 'series' in rtype:
        return [df]
    # Numpy array
    elif 'array' in rtype:
        return [df]
    # List of objects
    elif 'list' in rtype:
        return [df]

    raise TypeError(
        'Unsupported data type for dataframe columns: '
        f'{rtype}',
    )


def get_array_class(array: Any) -> Callable[..., Any]:
    """
    Get the array class for the current data format.

    """
    mod = inspect.getmodule(type(array))
    if mod:
        array_type = mod.__name__.split('.')[0]
    else:
        raise TypeError(f'Unsupported array type: {type(array)}')

    if array_type == 'polars':
        import polars as pl
        return pl.Series

    if array_type == 'pyarrow':
        import pyarrow as pa
        return pa.array

    if array_type == 'pandas':
        import pandas as pd
        return pd.Series

    if array_type == 'numpy':
        import numpy as np
        return np.array

    if isinstance(array, list):
        return list

    raise TypeError(f'Unsupported array type: {type(array)}')


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


def cancel_on_event(
    cancel_event: threading.Event,
) -> None:
    """
    Cancel the function call if the cancel event is set.

    Parameters
    ----------
    cancel_event : threading.Event
        The event to check for cancellation

    Raises
    ------
    asyncio.CancelledError
        If the cancel event is set

    """
    if cancel_event.is_set():
        task = asyncio.current_task()
        if task is not None:
            task.cancel()
        raise asyncio.CancelledError(
            'Function call was cancelled by client',
        )


RowIDs: TypeAlias = Sequence[int]
VectorInput: TypeAlias = Sequence[Tuple[Sequence[Any], Optional[Sequence[bool]]]]
ScalarInput: TypeAlias = Sequence[Sequence[Any]]
UDFInput = Union[VectorInput, ScalarInput]
VectorOutput: TypeAlias = List[Tuple[Sequence[Any], Optional[Sequence[bool]]]]
ScalarOutput: TypeAlias = List[Tuple[Any, ...]]
UDFOutput = Union[VectorOutput, ScalarOutput]


def scalar_in_scalar_out(
    func: Callable[..., Any],
    function_type: str = 'udf',
) -> Callable[
    [Event, Timer, RowIDs, ScalarInput],
    Coroutine[Any, Any, Tuple[RowIDs, ScalarOutput]],
]:
    """
    Create a scalar in, scalar out function endpoint.

    Parameters
    ----------
    func : Callable
        The function to call as the endpoint
    function_type : str, optional
        The type of function: 'udf' or 'tvf'

    Returns
    -------
    Callable
        The function endpoint

    """
    is_async = asyncio.iscoroutinefunction(func)
    is_udf = function_type == 'udf'

    async def do_scalar_in_scalar_out_func(
        cancel_event: threading.Event,
        timer: Timer,
        row_ids: RowIDs,
        rows: ScalarInput,
    ) -> Tuple[RowIDs, ScalarOutput]:
        """Call function on given rows of data."""
        cancel_on_event(cancel_event)

        async with (timer('call_function')):
            out_ids = []
            out_rows: ScalarOutput = []

            for i, row in zip(row_ids, rows):
                func_res = await func(*row) if is_async else func(*row)

                cancel_on_event(cancel_event)

                n_rows = extend_rows(out_rows, func_res)

                if is_udf and n_rows != 1:
                    raise ValueError('UDF must return a single value per input row')

                out_ids.extend([i] * n_rows)

            cancel_on_event(cancel_event)

            return out_ids, out_rows

    return do_scalar_in_scalar_out_func


def scalar_in_vector_out(
    func: Callable[..., Any],
    function_type: str = 'udf',
) -> Callable[
    [Event, Timer, RowIDs, ScalarInput],
    Coroutine[Any, Any, Tuple[RowIDs, VectorOutput]],
]:
    """
    Create a scalar in, vector out function endpoint.

    Parameters
    ----------
    func : Callable
        The function to call as the endpoint
    function_type : str, optional
        The type of function: 'udf' or 'tvf'

    Returns
    -------
    Callable
        The function endpoint

    """
    is_async = asyncio.iscoroutinefunction(func)
    is_udf = function_type == 'udf'

    async def do_scalar_in_vector_out_func(
        cancel_event: threading.Event,
        timer: Timer,
        row_ids: RowIDs,
        rows: ScalarInput,
    ) -> Tuple[RowIDs, VectorOutput]:
        """Call function on given rows of data."""
        cancel_on_event(cancel_event)

        async with (timer('call_function')):
            out_vectors = []
            out_ids = []
            for i, row in zip(row_ids, rows):
                func_res = await func(*row) if is_async else func(*row)

                cancel_on_event(cancel_event)

                res = get_dataframe_columns(func_res)

                ref = res[0][0] if isinstance(res[0], Masked) else res[0]
                if is_udf and len(ref) != 1:
                    raise ValueError('UDF must return a single value per input row')

                out_ids.extend([i] * len(ref))

                out_vectors.append([build_tuple(x) for x in res])

            cancel_on_event(cancel_event)

            # Concatenate vector results from all rows
            out = concatenate_vectors(out_vectors)

            return get_array_class(out[0][0][0])(out_ids), out

    return do_scalar_in_vector_out_func


def vector_in_vector_out(
    func: Callable[..., Any],
    function_type: str = 'udf',
) -> Callable[
    [Event, Timer, RowIDs, VectorInput],
    Coroutine[Any, Any, Tuple[RowIDs, VectorOutput]],
]:
    """
    Create a vector in, vector out function endpoint.

    Parameters
    ----------
    func : Callable
        The function to call as the endpoint
    function_type : str, optional
        The type of function: 'udf' or 'tvf'

    Returns
    -------
    Callable
        The function endpoint

    """
    masks = get_masked_params(func)
    is_async = asyncio.iscoroutinefunction(func)
    is_udf = function_type == 'udf'

    async def do_vector_in_vector_out_func(
        cancel_event: threading.Event,
        timer: Timer,
        row_ids: RowIDs,
        cols: VectorInput,
    ) -> Tuple[RowIDs, VectorOutput]:
        """Call function on given columns of data."""
        cancel_on_event(cancel_event)

        args = []

        async with timer('call_function'):
            # Remove masks from args if mask is None
            if cols and cols[0]:
                args = [x if m else x[0] for x, m in zip(cols, masks)]

            func_res = await func(*args) if is_async else func(*args)

            cancel_on_event(cancel_event)

            out = get_dataframe_columns(func_res)

            ref = out[0][0] if isinstance(out[0], Masked) else out[0]
            array_cls = get_array_class(ref)
            if is_udf:
                if len(ref) != len(row_ids):
                    raise ValueError('UDF must return a single value per input row')
                row_ids = array_cls(row_ids)
            else:
                row_ids = array_cls([row_ids[0]] * len(ref))

            return row_ids, [build_tuple(x) for x in out]

    return do_vector_in_vector_out_func


def vector_in_scalar_out(
    func: Callable[..., Any],
    function_type: str = 'udf',
) -> Callable[
    [Event, Timer, RowIDs, VectorInput],
    Coroutine[Any, Any, Tuple[RowIDs, ScalarOutput]],
]:
    """
    Create a vector in, scalar out function endpoint.

    Parameters
    ----------
    func : Callable
        The function to call as the endpoint
    function_type : str, optional
        The type of function: 'udf' or 'tvf'

    Returns
    -------
    Callable
        The function endpoint

    """
    masks = get_masked_params(func)
    is_async = asyncio.iscoroutinefunction(func)
    is_udf = function_type == 'udf'

    async def do_vector_in_scalar_out_func(
        cancel_event: threading.Event,
        timer: Timer,
        row_ids: RowIDs,
        cols: VectorInput,
    ) -> Tuple[RowIDs, ScalarOutput]:
        """Call function on given columns of data."""
        cancel_on_event(cancel_event)

        out_ids = []
        out_rows: ScalarOutput = []
        args = []

        async with timer('call_function'):
            # Remove masks from args if mask is None
            if cols and cols[0]:
                args = [x if m else x[0] for x, m in zip(cols, masks)]

            func_res = await func(*args) if is_async else func(*args)

            cancel_on_event(cancel_event)

            n_rows = extend_rows(out_rows, func_res)

            if is_udf:
                if n_rows != len(row_ids):
                    raise ValueError('UDF must return a single value per input row')
                out_ids = list(row_ids)
            else:
                out_ids.extend([row_ids[0]] * n_rows)

            return out_ids, out_rows

    return do_vector_in_scalar_out_func


def concatenate_vectors(segments: List[VectorOutput]) -> VectorOutput:
    """
    Concatenate lists of vectors with optional masks.

    Parameters
    ----------
    segments : List[VectorOutput]
        List of vectors to concatenate. Each vector is a list of tuples,
        where each tuple contains an array and an optional mask.

    Returns
    -------
    VectorOutput
        Concatenated vector with optional mask.

    Raises
    ------
    ValueError
        If masks are used on some but not all elements.

    """
    columns: List[List[Sequence[Any]]] = []
    masks: List[List[Sequence[bool]]] = []
    has_masks: List[bool] = []

    for s in segments:
        columns = [[]] * len(s)
        masks = [[]] * len(s)
        has_masks = [False] * len(s)
        for i, v in enumerate(s):
            columns[i].append(v[0])
            if v[1] is not None:
                masks[i].append(v[1])

    for i, mask in enumerate(masks):
        if mask and len(mask) != len(columns[i]):
            raise ValueError('Vector masks must be used on either all or no elements')
        if mask:
            has_masks[i] = True

    return [
        (_concatenate_arrays(c), _concatenate_arrays(m) if has_masks[i] else None)
        for i, (c, m) in enumerate(zip(columns, masks))
    ]


def _concatenate_arrays(
    arrays: Sequence[Sequence[Any]],
) -> Sequence[Any]:
    """
    Concatenate lists of arrays from various formats.

    Parameters
    ----------
    arrays : Sequence[Sequence[Any]]
        List of arrays to concatenate. Supported formats:
        - PyArrow arrays
        - NumPy arrays
        - Pandas Series
        - Polars Series
        - Python lists

    Returns
    -------
    Sequence[Any]
        Concatenated array in the same format as input arrays,
        or None if input is None

    Raises
    ------
    ValueError
        If arrays list contains a mix of None and non-None values
    TypeError
        If arrays contain mixed or unsupported types

    """
    if arrays[0] is None:
        raise ValueError('Cannot concatenate None arrays')

    mod = inspect.getmodule(type(arrays[0]))
    if mod:
        array_type = mod.__name__.split('.')[0]
    else:
        raise TypeError(f'Unsupported array type: {type(arrays[0])}')

    if array_type == 'numpy':
        import numpy as np
        return np.concatenate(arrays)

    if array_type == 'pyarrow':
        import pyarrow as pa
        return pa.concat_arrays(arrays)

    if array_type == 'pandas':
        import pandas as pd
        return pd.concat(arrays, ignore_index=True)

    if array_type == 'polars':
        import polars as pl
        return pl.concat(arrays)

    if isinstance(arrays[0], list):
        result: List[Any] = []
        for arr in arrays:
            result.extend(arr)
        return result

    raise TypeError(f'Unsupported array type: {type(arrays[0])}')


def build_udf_endpoint(
    func: Callable[..., Any],
    args_data_format: str,
    returns_data_format: str,
    function_type: str = 'udf',
) -> Callable[
    [Event, Timer, RowIDs, Any],
    Coroutine[Any, Any, Tuple[RowIDs, Any]],
]:
    """
    Build a UDF endpoint for scalar / list types (row-based).

    Parameters
    ----------
    func : Callable
        The function to call as the endpoint
    args_data_format : str
        The format of the argument values
    returns_data_format : str
        The format of the return values
    function_type : str, optional
        The type of function: 'udf' or 'tvf'

    Returns
    -------
    Callable
        The function endpoint

    """
    if args_data_format in ['scalar'] and returns_data_format in ['scalar']:
        return scalar_in_scalar_out(func, function_type=function_type)
    elif args_data_format in ['scalar'] and returns_data_format not in ['scalar']:
        return scalar_in_vector_out(func, function_type=function_type)
    elif args_data_format not in ['scalar'] and returns_data_format in ['scalar']:
        return vector_in_scalar_out(func, function_type=function_type)
    else:
        return vector_in_vector_out(func, function_type=function_type)


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
    database : str, optional
        The database to use for the function definition

    Returns
    -------
    (Callable, Dict[str, Any])

    """
    info: Dict[str, Any] = {}

    sig = get_signature(func, func_name=name)

    function_type = sig.get('function_type', 'udf')
    args_data_format = sig.get('args_data_format', 'scalar')
    returns_data_format = sig.get('returns_data_format', 'scalar')
    timeout = (
        func._singlestoredb_attrs.get('timeout') or  # type: ignore
        get_option('external_function.timeout')
    )

    do_func = build_udf_endpoint(
        func,
        args_data_format,
        returns_data_format,
        function_type=function_type,
    )

    do_func.__name__ = name
    do_func.__doc__ = func.__doc__

    # Store signature for generating CREATE FUNCTION calls
    info['signature'] = sig

    # Set data format
    info['args_data_format'] = args_data_format
    info['returns_data_format'] = returns_data_format

    # Set function type
    info['function_type'] = function_type

    # Set timeout
    info['timeout'] = max(timeout, 1)

    # Set async flag
    info['is_async'] = asyncio.iscoroutinefunction(func)

    # Setup argument types for rowdat_1 parser
    colspec = []
    for x in sig['args']:
        dtype = x['dtype'].replace('?', '')
        if dtype not in rowdat_1_type_map:
            raise TypeError(f'no data type mapping for {dtype}')
        colspec.append((x['name'], rowdat_1_type_map[dtype], x['transformer']))
    info['colspec'] = colspec

    # Setup return type
    returns = []
    for x in sig['returns']:
        dtype = x['dtype'].replace('?', '')
        if dtype not in rowdat_1_type_map:
            raise TypeError(f'no data type mapping for {dtype}')
        returns.append((x['name'], rowdat_1_type_map[dtype], x['transformer']))
    info['returns'] = returns

    return do_func, info


async def cancel_on_timeout(timeout: int) -> None:
    """Cancel request if it takes too long."""
    await asyncio.sleep(timeout)
    raise asyncio.CancelledError(
        'Function call was cancelled due to timeout',
    )


async def cancel_on_disconnect(
    receive: Callable[..., Awaitable[Any]],
) -> None:
    """Cancel request if client disconnects."""
    while True:
        message = await receive()
        if message.get('type', '') == 'http.disconnect':
            raise asyncio.CancelledError(
                'Function call was cancelled by client',
            )


async def cancel_all_tasks(tasks: Iterable[asyncio.Task[Any]]) -> None:
    """Cancel all tasks."""
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def start_counter() -> float:
    """Start a timer and return the start time."""
    return time.perf_counter()


def end_counter(start: float) -> float:
    """End a timer and return the elapsed time."""
    return time.perf_counter() - start


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
    name_prefix : str, optional
        Prefix to add to function names when registering with the database
    name_suffix : str, optional
        Suffix to add to function names when registering with the database
    function_database : str, optional
        The database to use for external function definitions.
    log_file : str, optional
        File path to write logs to instead of console. If None, logs are
        written to console. When specified, application logger handlers
        are replaced with a file handler.
    log_level : str, optional
        Logging level for the application logger. Valid values are 'info',
        'debug', 'warning', 'error'. Defaults to 'info'.
    disable_metrics : bool, optional
        Disable logging of function call metrics. Defaults to False.
    app_name : str, optional
        Name for the application instance. Used to create a logger-specific
        name. If not provided, a random name will be generated.

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
        status=500,
        headers=[(b'content-type', b'text/plain')],
    )

    # Timeout response start
    timeout_response_dict: Dict[str, Any] = dict(
        type='http.response.start',
        status=504,
        headers=[(b'content-type', b'text/plain')],
    )

    # Cancel response start
    cancel_response_dict: Dict[str, Any] = dict(
        type='http.response.start',
        status=503,
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
            load=rowdat_1.load_list,
            dump=rowdat_1.dump_list,
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
            load=jdata.load_list,
            dump=jdata.dump_list,
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
    status = ('status',)

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
        function_database: Optional[str] = None,
        log_file: Optional[str] = get_option('external_function.log_file'),
        log_level: str = get_option('external_function.log_level'),
        disable_metrics: bool = get_option('external_function.disable_metrics'),
        app_name: Optional[str] = get_option('external_function.app_name'),
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

        # Generate application name if not provided
        if app_name is None:
            app_name = f'udf_app_{secrets.token_hex(4)}'

        self.name = app_name

        # Create logger instance specific to this application
        self.logger = utils.get_logger(f'singlestoredb.functions.ext.asgi.{self.name}')

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
        self.function_database = function_database
        self.log_file = log_file
        self.log_level = log_level
        self.disable_metrics = disable_metrics

        # Configure logging
        self._configure_logging()

    def _configure_logging(self) -> None:
        """Configure logging based on the log_file settings."""
        # Set logger level
        self.logger.setLevel(getattr(logging, self.log_level.upper()))

        # Remove all existing handlers to ensure clean configuration
        self.logger.handlers.clear()

        # Configure log file if specified
        if self.log_file:
            # Create file handler
            file_handler = logging.FileHandler(self.log_file)
            file_handler.setLevel(getattr(logging, self.log_level.upper()))

            # Use JSON formatter for file logging
            formatter = utils.JSONFormatter()
            file_handler.setFormatter(formatter)

            # Add the handler to the logger
            self.logger.addHandler(file_handler)
        else:
            # For console logging, create a new stream handler with JSON formatter
            console_handler = logging.StreamHandler()
            console_handler.setLevel(getattr(logging, self.log_level.upper()))
            console_handler.setFormatter(utils.JSONFormatter())
            self.logger.addHandler(console_handler)

        # Prevent propagation to avoid duplicate or differently formatted messages
        self.logger.propagate = False

    def get_uvicorn_log_config(self) -> Dict[str, Any]:
        """
        Create uvicorn log config that matches the Application's logging format.

        This method returns the log configuration used by uvicorn, allowing external
        users to match the logging format of the Application class.

        Returns
        -------
        Dict[str, Any]
            Log configuration dictionary compatible with uvicorn's log_config parameter

        """
        log_config = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'json': {
                    '()': 'singlestoredb.functions.ext.utils.JSONFormatter',
                },
            },
            'handlers': {
                'default': {
                    'class': (
                        'logging.FileHandler' if self.log_file
                        else 'logging.StreamHandler'
                    ),
                    'formatter': 'json',
                },
            },
            'loggers': {
                'uvicorn': {
                    'handlers': ['default'],
                    'level': self.log_level.upper(),
                    'propagate': False,
                },
                'uvicorn.error': {
                    'handlers': ['default'],
                    'level': self.log_level.upper(),
                    'propagate': False,
                },
                'uvicorn.access': {
                    'handlers': ['default'],
                    'level': self.log_level.upper(),
                    'propagate': False,
                },
            },
        }

        # Add filename to file handler if log file is specified
        if self.log_file:
            log_config['handlers']['default']['filename'] = self.log_file  # type: ignore

        return log_config

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
        request_id = str(uuid.uuid4())

        timer = Timer(
            app_name=self.name,
            id=request_id,
            timestamp=datetime.datetime.now(
                datetime.timezone.utc,
            ).strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        )
        call_timer = Timer(
            app_name=self.name,
            id=request_id,
            timestamp=datetime.datetime.now(
                datetime.timezone.utc,
            ).strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        )

        if scope['type'] != 'http':
            raise ValueError(f"Expected HTTP scope, got {scope['type']}")

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
        ignore_cancel = headers.get(b's2-ef-ignore-cancel', b'false') == b'true'

        timer.metadata['function'] = func_name.decode('utf-8') if func_name else ''
        call_timer.metadata['function'] = timer.metadata['function']

        func = None
        func_info: Dict[str, Any] = {}
        if func_endpoint is not None:
            func, func_info = func_endpoint

        # Call the endpoint
        if method == 'POST' and func is not None and path == self.invoke_path:

            self.logger.info(
                'Function call initiated',
                extra={
                    'app_name': self.name,
                    'request_id': request_id,
                    'function_name': func_name.decode('utf-8'),
                    'content_type': content_type.decode('utf-8'),
                    'accepts': accepts.decode('utf-8'),
                },
            )

            args_data_format = func_info['args_data_format']
            returns_data_format = func_info['returns_data_format']
            data = []
            more_body = True
            with timer('receive_data'):
                while more_body:
                    request = await receive()
                    if request.get('type', '') == 'http.disconnect':
                        raise RuntimeError('client disconnected')
                    data.append(request['body'])
                    more_body = request.get('more_body', False)

            data_version = headers.get(b's2-ef-version', b'')
            input_handler = self.handlers[(content_type, data_version, args_data_format)]
            output_handler = self.handlers[(accepts, data_version, returns_data_format)]

            try:
                all_tasks = []
                result = []

                cancel_event = threading.Event()

                with timer('parse_input'):
                    inputs = input_handler['load'](  # type: ignore
                        func_info['colspec'], b''.join(data),
                    )

                func_task = asyncio.create_task(
                    func(cancel_event, call_timer, *inputs)
                    if func_info['is_async']
                    else to_thread(
                        lambda: asyncio.run(
                            func(cancel_event, call_timer, *inputs),
                        ),
                    ),
                )
                disconnect_task = asyncio.create_task(
                    asyncio.sleep(int(1e9))
                    if ignore_cancel else cancel_on_disconnect(receive),
                )
                timeout_task = asyncio.create_task(
                    cancel_on_timeout(func_info['timeout']),
                )

                all_tasks += [func_task, disconnect_task, timeout_task]

                async with timer('function_wrapper'):
                    done, pending = await asyncio.wait(
                        all_tasks, return_when=asyncio.FIRST_COMPLETED,
                    )

                await cancel_all_tasks(pending)

                for task in done:
                    if task is disconnect_task:
                        cancel_event.set()
                        raise asyncio.CancelledError(
                            'Function call was cancelled by client disconnect',
                        )

                    elif task is timeout_task:
                        cancel_event.set()
                        raise asyncio.TimeoutError(
                            'Function call was cancelled due to timeout',
                        )

                    elif task is func_task:
                        result.extend(task.result())

                with timer('format_output'):
                    body = output_handler['dump'](
                        func_info['returns'], *result,  # type: ignore
                    )

                await send(output_handler['response'])

            except asyncio.TimeoutError:
                self.logger.exception(
                    'Function call timeout',
                    extra={
                        'app_name': self.name,
                        'request_id': request_id,
                        'function_name': func_name.decode('utf-8'),
                        'timeout': func_info['timeout'],
                    },
                )
                body = (
                    'TimeoutError: Function call timed out after ' +
                    str(func_info['timeout']) +
                    ' seconds'
                ).encode('utf-8')
                await send(self.timeout_response_dict)

            except asyncio.CancelledError:
                self.logger.exception(
                    'Function call cancelled',
                    extra={
                        'app_name': self.name,
                        'request_id': request_id,
                        'function_name': func_name.decode('utf-8'),
                    },
                )
                body = b'CancelledError: Function call was cancelled'
                await send(self.cancel_response_dict)

            except Exception as e:
                self.logger.exception(
                    'Function call error',
                    extra={
                        'app_name': self.name,
                        'request_id': request_id,
                        'function_name': func_name.decode('utf-8'),
                        'exception_type': type(e).__name__,
                    },
                )
                msg = traceback.format_exc().strip().split(' File ')[-1]
                if msg.startswith('"/tmp/ipykernel_'):
                    msg = 'Line ' + msg.split(', line ')[-1]
                else:
                    msg = 'File ' + msg
                body = msg.encode('utf-8')
                await send(self.error_response_dict)

            finally:
                await cancel_all_tasks(all_tasks)

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
                            database=self.function_database or None,
                        ),
                    )
            body = '\n'.join(syntax).encode('utf-8')

            await send(self.text_response_dict)

        # Return function info
        elif method == 'GET' and (path == self.show_function_info_path or not path):
            functions = self.get_function_info()
            body = json.dumps(dict(functions=functions)).encode('utf-8')
            await send(self.text_response_dict)

        # Return status
        elif method == 'GET' and path == self.status:
            body = json.dumps(dict(status='ok')).encode('utf-8')
            await send(self.text_response_dict)

        # Path not found
        else:
            body = b''
            await send(self.path_not_found_response_dict)

        # Send body
        with timer('send_response'):
            out = self.body_response_dict.copy()
            out['body'] = body
            await send(out)

        for k, v in call_timer.metrics.items():
            timer.metrics[k] = v

        if not self.disable_metrics:
            metrics = timer.finish()
            self.logger.info(
                'Function call metrics',
                extra={
                    'app_name': self.name,
                    'request_id': request_id,
                    'function_name': timer.metadata.get('function', ''),
                    'metrics': metrics,
                },
            )

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
        if self.function_database:
            database_prefix = escape_name(self.function_database) + '.'
            cur.execute(f'SHOW FUNCTIONS IN {escape_name(self.function_database)}')
        else:
            database_prefix = ''
            cur.execute('SHOW FUNCTIONS')

        for row in list(cur):
            name, ftype, link = row[0], row[1], row[-1]
            # Only look at external functions
            if 'external' not in ftype.lower():
                continue
            # See if function URL matches url
            cur.execute(f'SHOW CREATE FUNCTION {database_prefix}{escape_name(name)}')
            for fname, _, code, *_ in list(cur):
                m = re.search(r" (?:\w+) (?:SERVICE|MANAGED) '([^']+)'", code)
                if m and m.group(1) == self.url:
                    funcs.add(f'{database_prefix}{escape_name(fname)}')
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

        # Generate CREATE FUNCTION SQL for each function using get_create_functions
        create_sqls = self.get_create_functions(replace=True)
        sql_map = {}
        for (_, info), sql in zip(self.endpoints.values(), create_sqls):
            sig = info['signature']
            sql_map[sig['name']] = sql

        for key, (func, info) in self.endpoints.items():
            # Get info from docstring
            doc_summary = ''
            doc_long_description = ''
            doc_params = {}
            doc_returns = None
            doc_examples = []
            if func.__doc__:
                try:
                    docs = parse(func.__doc__)
                    doc_params = {p.arg_name: p for p in docs.params}
                    doc_returns = docs.returns
                    if not docs.short_description and docs.long_description:
                        doc_summary = docs.long_description or ''
                    else:
                        doc_summary = docs.short_description or ''
                        doc_long_description = docs.long_description or ''
                    for ex in docs.examples:
                        ex_dict: Dict[str, Any] = {
                            'description': None,
                            'code': None,
                            'output': None,
                        }
                        if ex.description:
                            ex_dict['description'] = ex.description
                        if ex.snippet:
                            code, output = [], []
                            for line in ex.snippet.split('\n'):
                                line = line.rstrip()
                                if re.match(r'^(\w+>|>>>|\.\.\.)', line):
                                    code.append(line)
                                else:
                                    output.append(line)
                            ex_dict['code'] = '\n'.join(code) or None
                            ex_dict['output'] = '\n'.join(output) or None
                        if ex.post_snippet:
                            ex_dict['postscript'] = ex.post_snippet
                        doc_examples.append(ex_dict)

                except Exception as e:
                    self.logger.warning(
                        'Could not parse docstring for function',
                        extra={
                            'app_name': self.name,
                            'function_name': key.decode('utf-8'),
                            'error': str(e),
                        },
                    )

            if not func_name or key == func_name:
                sig = info['signature']
                args = []

                # Function arguments
                for i, a in enumerate(sig.get('args', [])):
                    name = a['name']
                    dtype = a['dtype']
                    nullable = '?' in dtype
                    args.append(
                        dict(
                            name=name,
                            dtype=dtype.replace('?', ''),
                            nullable=nullable,
                            description=(doc_params[name].description or '')
                            if name in doc_params else '',
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
                            description=doc_returns.description
                            if doc_returns else '',
                        ),
                    )
                    if a.get('name', None):
                        returns[-1]['name'] = a['name']
                    if a.get('default', no_default) is not no_default:
                        returns[-1]['default'] = a['default']

                sql = sql_map.get(sig['name'], '')
                functions[sig['name']] = dict(
                    args=args,
                    returns=returns,
                    function_type=info['function_type'],
                    sql_statement=sql,
                    summary=doc_summary,
                    long_description=doc_long_description,
                    examples=doc_examples,
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
                    database=self.function_database or None,
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
                        cur.execute(f'DROP FUNCTION IF EXISTS {fname}')
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
                    cur.execute(f'DROP FUNCTION IF EXISTS {fname}')
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
                b's2-ef-ignore-cancel': b'true',
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
            '--log-file', metavar='filepath',
            default=defaults.get(
                'log_file',
                get_option('external_function.log_file'),
            ),
            help='File path to write logs to instead of console',
        )
        parser.add_argument(
            '--disable-metrics', action='store_true',
            default=defaults.get(
                'disable_metrics',
                get_option('external_function.disable_metrics'),
            ),
            help='Disable logging of function call metrics',
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
            '--function-database', metavar='function_database',
            default=defaults.get(
                'function_database',
                get_option('external_function.function_database'),
            ),
            help='Database to use for the function definition',
        )
        parser.add_argument(
            '--app-name', metavar='app_name',
            default=defaults.get(
                'app_name',
                get_option('external_function.app_name'),
            ),
            help='Name for the application instance',
        )
        parser.add_argument(
            'functions', metavar='module.or.func.path', nargs='*',
            help='functions or modules to export in UDF server',
        )

        args = parser.parse_args(argv)

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
        function_database=args.function_database or None,
        log_file=args.log_file,
        log_level=args.log_level,
        disable_metrics=args.disable_metrics,
        app_name=args.app_name,
    )

    funcs = app.get_create_functions(replace=args.replace_existing)
    if not funcs:
        raise RuntimeError('no functions specified')

    for f in funcs:
        app.logger.info(f)

    try:
        if args.db:
            app.logger.info('Registering functions with database')
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

        # Configure uvicorn logging to use JSON format matching Application's format
        app_args['log_config'] = app.get_uvicorn_log_config()

        if use_async:
            asyncio.create_task(_run_uvicorn(uvicorn, app, app_args, db=args.db))
        else:
            uvicorn.run(app, **app_args)

    finally:
        if not use_async and args.db:
            app.logger.info('Dropping functions from database')
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
        app.logger.info('Dropping functions from database')
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
