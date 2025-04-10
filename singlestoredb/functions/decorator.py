from __future__ import annotations

import functools
import inspect
from typing import Any
from typing import Callable
from typing import List
from typing import Optional
from typing import Type
from typing import Union


ParameterType = Union[
    str,
    Callable[..., str],
    List[Union[str, Callable[..., str]]],
    Type[Any],
]

ReturnType = ParameterType


def expand_types(args: Any) -> Optional[Union[List[str], Type[Any]]]:
    """Expand the types for the function arguments / return values."""
    if args is None:
        return None

    # SQL string
    if isinstance(args, str):
        return [args]

    # General way of accepting pydantic.BaseModel, NamedTuple, TypedDict
    elif inspect.isclass(args):
        return args

    # Callable that returns a SQL string
    elif callable(args):
        out = args()
        if not isinstance(out, str):
            raise TypeError(f'unrecognized type for parameter: {args}')
        return [out]

    # List of SQL strings or callables
    else:
        new_args = []
        for arg in args:
            if isinstance(arg, str):
                new_args.append(arg)
            elif callable(arg):
                new_args.append(arg())
            else:
                raise TypeError(f'unrecognized type for parameter: {arg}')
        return new_args


def _func(
    func: Optional[Callable[..., Any]] = None,
    *,
    name: Optional[str] = None,
    args: Optional[ParameterType] = None,
    returns: Optional[ReturnType] = None,
    include_masks: bool = False,
    function_type: str = 'udf',
) -> Callable[..., Any]:
    """Generic wrapper for UDF and TVF decorators."""

    _singlestoredb_attrs = {  # type: ignore
        k: v for k, v in dict(
            name=name,
            args=expand_types(args),
            returns=expand_types(returns),
            include_masks=include_masks,
            function_type=function_type,
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
    args: Optional[ParameterType] = None,
    returns: Optional[ReturnType] = None,
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
    args : str | Callable | List[str | Callable], optional
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
        include_masks=include_masks,
        function_type='udf',
    )


def tvf(
    func: Optional[Callable[..., Any]] = None,
    *,
    name: Optional[str] = None,
    args: Optional[ParameterType] = None,
    returns: Optional[ReturnType] = None,
    include_masks: bool = False,
) -> Callable[..., Any]:
    """
    Apply attributes to a TVF.

    Parameters
    ----------
    func : callable, optional
        The TVF to apply parameters to
    name : str, optional
        The name to use for the TVF in the database
    args : str | Callable | List[str | Callable], optional
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
        include_masks=include_masks,
        function_type='tvf',
    )
