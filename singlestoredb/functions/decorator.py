import datetime
import functools
import string
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from . import dtypes
from .dtypes import DataType

python_type_map: Dict[Any, Callable[..., str]] = {
    str: dtypes.TEXT,
    int: dtypes.BIGINT,
    float: dtypes.DOUBLE,
    bool: dtypes.BOOL,
    bytes: dtypes.BINARY,
    bytearray: dtypes.BINARY,
    datetime.datetime: dtypes.DATETIME,
    datetime.date: dtypes.DATE,
    datetime.timedelta: dtypes.TIME,
}


def listify(x: Any) -> List[Any]:
    """Make sure sure value is a list."""
    if x is None:
        return []
    if isinstance(x, (list, tuple, set)):
        return list(x)
    return [x]


def _func(
    func: Optional[Callable[..., Any]] = None,
    *,
    name: Optional[str] = None,
    args: Optional[Union[DataType, List[DataType], Dict[str, DataType]]] = None,
    returns: Optional[Union[str, List[DataType], List[type]]] = None,
    data_format: Optional[str] = None,
    include_masks: bool = False,
    function_type: str = 'udf',
    output_fields: Optional[List[str]] = None,
) -> Callable[..., Any]:
    """Generic wrapper for UDF and TVF decorators."""
    if args is None:
        pass
    elif isinstance(args, (list, tuple)):
        args = list(args)
        for i, item in enumerate(args):
            if args[i] in python_type_map:
                args[i] = python_type_map[args[i]]()
            elif callable(item):
                args[i] = item()
        for item in args:
            if not isinstance(item, str):
                raise TypeError(f'unrecognized type for parameter: {item}')
    elif isinstance(args, dict):
        args = dict(args)
        for k, v in list(args.items()):
            if args[k] in python_type_map:
                args[k] = python_type_map[args[k]]()
            elif callable(v):
                args[k] = v()
        for item in args.values():
            if not isinstance(item, str):
                raise TypeError(f'unrecognized type for parameter: {item}')
    elif args in python_type_map:
        args = python_type_map[args]()
    elif callable(args):
        args = args()
    elif isinstance(args, str):
        args = args
    else:
        raise TypeError(f'unrecognized data type for args: {args}')

    if returns is None:
        pass
    elif isinstance(returns, (list, tuple)):
        returns = list(returns)
        for i, item in enumerate(returns):
            if item in python_type_map:
                returns[i] = python_type_map[item]()
            elif callable(item):
                returns[i] = item()
        for item in returns:
            if not isinstance(item, str):
                raise TypeError(f'unrecognized return type: {item}')
    elif returns in python_type_map:
        returns = python_type_map[returns]()
    elif callable(returns):
        returns = returns()
    elif isinstance(returns, str):
        returns = returns
    else:
        raise TypeError(f'unrecognized return type: {returns}')

    if returns is None:
        pass
    elif isinstance(returns, list):
        for item in returns:
            if not isinstance(item, str):
                raise TypeError(f'unrecognized return type: {item}')
    elif not isinstance(returns, str):
        raise TypeError(f'unrecognized return type: {returns}')

    if not output_fields:
        if isinstance(returns, list):
            output_fields = []
            for i, _ in enumerate(returns):
                output_fields.append(string.ascii_letters[i])
        else:
            output_fields = [string.ascii_letters[0]]

    if isinstance(returns, list) and len(output_fields) != len(returns):
        raise ValueError(
            'The number of output fields must match the number of return types',
        )

    if include_masks and data_format == 'python':
        raise RuntimeError(
            'include_masks is only valid when using '
            'vectors for input parameters',
        )

    _singlestoredb_attrs = {  # type: ignore
        k: v for k, v in dict(
            name=name,
            args=args,
            returns=returns,
            data_format=data_format,
            include_masks=include_masks,
            function_type=function_type,
            output_fields=output_fields,
        ).items() if v is not None
    }

    # No func was specified, this is an uncalled decorator that will get
    # called later, so the wrapper much be created with the func passed
    # in at that time.
    if func is None:
        def decorate(func: Callable[..., Any]) -> Callable[..., Any]:
            def wrapper(*args: Any, **kwargs: Any) -> Callable[..., Any]:
                return func(*args, **kwargs)  # type: ignore
            wrapper._singlestoredb_attrs = _singlestoredb_attrs  # type: ignore
            return functools.wraps(func)(wrapper)
        return decorate

    def wrapper(*args: Any, **kwargs: Any) -> Callable[..., Any]:
        return func(*args, **kwargs)  # type: ignore

    wrapper._singlestoredb_attrs = _singlestoredb_attrs  # type: ignore

    return functools.wraps(func)(wrapper)


def udf(
    func: Optional[Callable[..., Any]] = None,
    *,
    name: Optional[str] = None,
    args: Optional[Union[DataType, List[DataType], Dict[str, DataType]]] = None,
    returns: Optional[Union[str, List[DataType], List[type]]] = None,
    data_format: Optional[str] = None,
    include_masks: bool = False,
) -> Callable[..., Any]:
    """
    Apply attributes to a UDF.

    Parameters
    ----------
    func : callable, optional
        The UDF to apply parameters to
    name : str, optional
        The name to use for the UDF in the database
    args : str | Callable | List[str | Callable] | Dict[str, str | Callable], optional
        Specifies the data types of the function arguments. Typically,
        the function data types are derived from the function parameter
        annotations. These annotations can be overridden. If the function
        takes a single type for all parameters, `args` can be set to a
        SQL string describing all parameters. If the function takes more
        than one parameter and all of the parameters are being manually
        defined, a list of SQL strings may be used (one for each parameter).
        A dictionary of SQL strings may be used to specify a parameter type
        for a subset of parameters; the keys are the names of the
        function parameters. Callables may also be used for datatypes. This
        is primarily for using the functions in the ``dtypes`` module that
        are associated with SQL types with all default options (e.g., ``dt.FLOAT``).
    returns : str, optional
        Specifies the return data type of the function. If not specified,
        the type annotation from the function is used.
    data_format : str, optional
        The data format of each parameter: python, pandas, arrow, polars
    include_masks : bool, optional
        Should boolean masks be included with each input parameter to indicate
        which elements are NULL? This is only used when a input parameters are
        configured to a vector type (numpy, pandas, polars, arrow).

    Returns
    -------
    Callable

    """
    return _func(
        func=func,
        name=name,
        args=args,
        returns=returns,
        data_format=data_format,
        include_masks=include_masks,
        function_type='udf',
    )


udf.pandas = functools.partial(udf, data_format='pandas')  # type: ignore
udf.polars = functools.partial(udf, data_format='polars')  # type: ignore
udf.arrow = functools.partial(udf, data_format='arrow')  # type: ignore
udf.numpy = functools.partial(udf, data_format='numpy')  # type: ignore


def tvf(
    func: Optional[Callable[..., Any]] = None,
    *,
    name: Optional[str] = None,
    args: Optional[Union[DataType, List[DataType], Dict[str, DataType]]] = None,
    returns: Optional[Union[str, List[DataType], List[type]]] = None,
    data_format: Optional[str] = None,
    include_masks: bool = False,
    output_fields: Optional[List[str]] = None,
) -> Callable[..., Any]:
    """
    Apply attributes to a TVF.

    Parameters
    ----------
    func : callable, optional
        The TVF to apply parameters to
    name : str, optional
        The name to use for the TVF in the database
    args : str | Callable | List[str | Callable] | Dict[str, str | Callable], optional
        Specifies the data types of the function arguments. Typically,
        the function data types are derived from the function parameter
        annotations. These annotations can be overridden. If the function
        takes a single type for all parameters, `args` can be set to a
        SQL string describing all parameters. If the function takes more
        than one parameter and all of the parameters are being manually
        defined, a list of SQL strings may be used (one for each parameter).
        A dictionary of SQL strings may be used to specify a parameter type
        for a subset of parameters; the keys are the names of the
        function parameters. Callables may also be used for datatypes. This
        is primarily for using the functions in the ``dtypes`` module that
        are associated with SQL types with all default options (e.g., ``dt.FLOAT``).
    returns : str, optional
        Specifies the return data type of the function. If not specified,
        the type annotation from the function is used.
    data_format : str, optional
        The data format of each parameter: python, pandas, arrow, polars
    include_masks : bool, optional
        Should boolean masks be included with each input parameter to indicate
        which elements are NULL? This is only used when a input parameters are
        configured to a vector type (numpy, pandas, polars, arrow).
    output_fields : List[str], optional
        The names of the output fields for the TVF. If not specified, the
        names are generated.

    Returns
    -------
    Callable

    """
    return _func(
        func=func,
        name=name,
        args=args,
        returns=returns,
        data_format=data_format,
        include_masks=include_masks,
        function_type='tvf',
        output_fields=output_fields,
    )


tvf.pandas = functools.partial(tvf, data_format='pandas')  # type: ignore
tvf.polars = functools.partial(tvf, data_format='polars')  # type: ignore
tvf.arrow = functools.partial(tvf, data_format='arrow')  # type: ignore
tvf.numpy = functools.partial(tvf, data_format='numpy')  # type: ignore
