#!/usr/bin/env python3
'''
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
$ SINGLESTOREDB_EXT_FUNCTIONS='myfuncs.[percentage_90,percentage_95]' \
    uvicorn --factory singlestoredb.functions.ext:create_app

'''
import importlib.util
import io
import itertools
import os
import urllib
from types import ModuleType
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Union

from . import arrow
from . import json as jdata
from . import rowdat_1
from ... import connection
from ...mysql.constants import FIELD_TYPE as ft
from ..signature import get_signature
from ..signature import signature_to_sql

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
    '''
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

    '''
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


def make_func(name: str, func: Callable[..., Any]) -> Callable[..., Any]:
    '''
    Make a function endpoint.

    Parameters
    ----------
    name : str
        Name of the function to create
    func : Callable
        The function to call as the endpoint

    Returns
    -------
    Callable

    '''
    attrs = getattr(func, '_singlestoredb_attrs', {})
    data_format = attrs.get('data_format') or 'python'
    include_masks = attrs.get('include_masks', False)

    if data_format == 'python':
        async def do_func(
            row_ids: Sequence[int],
            rows: Sequence[Sequence[Any]],
        ) -> Tuple[
            Sequence[int],
            List[Tuple[Any]],
        ]:
            '''Call function on given rows of data.'''
            return row_ids, list(zip(func_map(func, rows)))

    else:
        # Vector formats use the same function wrapper
        async def do_func(  # type: ignore
            row_ids: Sequence[int],
            cols: Sequence[Tuple[Sequence[Any], Optional[Sequence[bool]]]],
        ) -> Tuple[Sequence[int], List[Tuple[Any, ...]]]:
            '''Call function on given cols of data.'''
            # TODO: only supports a single return value
            if include_masks:
                out = func(*cols)
                assert isinstance(out, tuple)
                return row_ids, [out]
            return row_ids, [(func(*[x[0] for x in cols]), None)]

    do_func.__name__ = name
    do_func.__doc__ = func.__doc__

    sig = get_signature(func, name=name)

    # Store signature for generating CREATE FUNCTION calls
    do_func._ext_func_signature = sig  # type: ignore

    # Set data format
    do_func._ext_func_data_format = data_format  # type: ignore

    # Setup argument types for rowdat_1 parser
    colspec = []
    for x in sig['args']:
        dtype = x['dtype'].replace('?', '')
        if dtype not in rowdat_1_type_map:
            raise TypeError(f'no data type mapping for {dtype}')
        colspec.append((x['name'], rowdat_1_type_map[dtype]))
    do_func._ext_func_colspec = colspec  # type: ignore

    # Setup return type
    dtype = sig['returns']['dtype'].replace('?', '')
    if dtype not in rowdat_1_type_map:
        raise TypeError(f'no data type mapping for {dtype}')
    do_func._ext_func_returns = [rowdat_1_type_map[dtype]]  # type: ignore

    return do_func


def create_app(  # noqa: C901
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


) -> Callable[..., Any]:
    '''
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

    Returns
    -------
    Callable : the application request handler

    '''

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
    for funcs in itertools.chain(specs):

        if isinstance(funcs, str):
            # Module name
            if importlib.util.find_spec(funcs) is not None:
                items = importlib.import_module(funcs)
                for x in vars(items).values():
                    if not hasattr(x, '_singlestoredb_attrs'):
                        continue
                    name = x._singlestoredb_attrs.get('name', x.__name__)
                    func = make_func(name, x)
                    endpoints[name.encode('utf-8')] = func

            # Fully qualified function name
            else:
                pkg_path, func_names = funcs.rsplit('.', 1)
                pkg = importlib.import_module(pkg_path)

                # Add endpoint for each exported function
                for name, alias in get_func_names(func_names):
                    item = getattr(pkg, name)
                    func = make_func(alias, item)
                    endpoints[alias.encode('utf-8')] = func

        elif isinstance(funcs, ModuleType):
            for x in vars(funcs).values():
                if not hasattr(x, '_singlestoredb_attrs'):
                    continue
                name = x._singlestoredb_attrs.get('name', x.__name__)
                func = make_func(name, x)
                endpoints[name.encode('utf-8')] = func

        else:
            alias = funcs.__name__
            func = make_func(alias, funcs)
            endpoints[alias.encode('utf-8')] = func

    # Plain text response start
    text_response_dict: Dict[str, Any] = dict(
        type='http.response.start',
        status=200,
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
        (b'application/octet-stream', b'1.0', 'python'): dict(
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
        (b'application/json', b'1.0', 'python'): dict(
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
        (b'application/vnd.apache.arrow.file', b'1.0', 'python'): dict(
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

    async def app(
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
        func = endpoints.get(func_name)

        # Call the endpoint
        if method == 'POST' and func is not None and path == invoke_path:
            data_format = func._ext_func_data_format  # type: ignore
            data = []
            more_body = True
            while more_body:
                request = await receive()
                data.append(request['body'])
                more_body = request.get('more_body', False)

            data_version = headers.get(b's2-ef-version', b'')
            input_handler = handlers[(content_type, data_version, data_format)]
            output_handler = handlers[(accepts, data_version, data_format)]

            out = await func(
                *input_handler['load'](
                    func._ext_func_colspec, b''.join(data),  # type: ignore
                ),
            )
            body = output_handler['dump'](func._ext_func_returns, *out)  # type: ignore

            await send(output_handler['response'])

        # Handle api reflection
        elif method == 'GET' and path == show_create_function_path:
            host = headers.get(b'host', b'localhost:80')
            url = f'{scope["scheme"]}://{host.decode("utf-8")}/invoke'
            data_format = 'json' if b'json' in content_type else 'rowdat_1'

            syntax = []
            for key, endpoint in endpoints.items():
                if not func_name or key == func_name:
                    syntax.append(
                        signature_to_sql(
                            endpoint._ext_func_signature,  # type: ignore
                            base_url=url,
                            data_format=data_format,
                        ),
                    )
            body = '\n'.join(syntax).encode('utf-8')

            await send(text_response_dict)

        # Path not found
        else:
            body = b''
            await send(path_not_found_response_dict)

        # Send body
        out = body_response_dict.copy()
        out['body'] = body
        await send(out)

    def show_create_functions(
        base_url: str = 'http://localhost:8000',
        data_format: str = 'rowdat_1',
    ) -> List[str]:
        out = []
        for key, endpoint in endpoints.items():
            out.append(
                signature_to_sql(
                    endpoint._ext_func_signature,  # type: ignore
                    base_url=urllib.parse.urljoin(base_url, '/invoke'),
                    data_format=data_format,
                ),
            )
        return out

    app.show_create_functions = show_create_functions  # type: ignore

    def register_functions(
        *connection_args: Any,
        base_url: str = 'http://localhost:8000',
        data_format: str = 'rowdat_1',
        **connection_kwargs: Any,
    ) -> None:
        with connection.connect(*connection_args, **connection_kwargs) as conn:
            with conn.cursor() as cur:
                for func in app.show_create_functions(  # type: ignore
                                base_url=base_url,
                                data_format=data_format,
                ):  # type: ignore
                    cur.execute(func)

    app.register_functions = register_functions  # type: ignore

    def drop_functions(
        *connection_args: Any,
        **connection_kwargs: Any,
    ) -> None:
        with connection.connect(*connection_args, **connection_kwargs) as conn:
            with conn.cursor() as cur:
                for key in endpoints.keys():
                    cur.execute(f'DROP FUNCTION IF EXISTS `{key.decode("utf8")}`')

    app.drop_functions = drop_functions  # type: ignore

    async def call(
        name: str,
        data_in: io.BytesIO,
        data_out: io.BytesIO,
        data_format: str = 'rowdat_1',
        data_version: str = '1.0',
    ) -> None:

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

        await app(scope, receive, send)

    app.call = call  # type: ignore

    return app
