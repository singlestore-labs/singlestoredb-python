import functools
import inspect
import typing
from typing import Any
from typing import Callable
from typing import List
from typing import Optional
from typing import Type
from typing import Union

from . import utils
from .dtypes import SQLString


ParameterType = Union[
    str,
    Callable[..., SQLString],
    List[Union[str, Callable[..., SQLString]]],
    Type[Any],
]

ReturnType = ParameterType


def is_valid_type(obj: Any) -> bool:
    """Check if the object is a valid type for a schema definition."""
    if not inspect.isclass(obj):
        return False

    if utils.is_typeddict(obj):
        return True

    if utils.is_namedtuple(obj):
        return True

    if utils.is_dataclass(obj):
        return True

    # We don't want to import pydantic here, so we check if
    # the class is a subclass
    if utils.is_pydantic(obj):
        return True

    return False


def is_valid_callable(obj: Any) -> bool:
    """Check if the object is a valid callable for a parameter type."""
    if not callable(obj):
        return False

    returns = inspect.get_annotations(obj).get('return', None)

    if inspect.isclass(returns) and issubclass(returns, str):
        return True

    raise TypeError(
        f'callable {obj} must return a str, '
        f'but got {returns}',
    )


def verify_mask(obj: Any) -> bool:
    """Verify that the object is a tuple of two vector types."""
    if typing.get_origin(obj) is not tuple or len(typing.get_args(obj)) != 2:
        raise TypeError(
            f'Expected a tuple of two vector types, but got {type(obj)}',
        )

    args = typing.get_args(obj)

    if not utils.is_vector(args[0]):
        raise TypeError(
            f'Expected a vector type for the first element, but got {args[0]}',
        )

    if not utils.is_vector(args[1]):
        raise TypeError(
            f'Expected a vector type for the second element, but got {args[1]}',
        )

    return True


def verify_masks(obj: Callable[..., Any]) -> bool:
    """Verify that the function parameters and return value are all masks."""
    ann = utils.get_annotations(obj)
    for name, value in ann.items():
        if not verify_mask(value):
            raise TypeError(
                f'Expected a vector type for the parameter {name} '
                f'in function {obj.__name__}, but got {value}',
            )
    return True


def expand_types(args: Any) -> Optional[Union[List[str], Type[Any]]]:
    """Expand the types for the function arguments / return values."""
    if args is None:
        return None

    # SQL string
    if isinstance(args, str):
        return [args]

    # General way of accepting pydantic.BaseModel, NamedTuple, TypedDict
    elif is_valid_type(args):
        return args

    # List of SQL strings or callables
    elif isinstance(args, list):
        new_args = []
        for arg in args:
            if isinstance(arg, str):
                new_args.append(arg)
            elif callable(arg):
                new_args.append(arg())
            else:
                raise TypeError(f'unrecognized type for parameter: {arg}')
        return new_args

    # Callable that returns a SQL string
    elif is_valid_callable(args):
        out = args()
        if not isinstance(out, str):
            raise TypeError(f'unrecognized type for parameter: {args}')
        return [out]

    raise TypeError(f'unrecognized type for parameter: {args}')


def _func(
    func: Optional[Callable[..., Any]] = None,
    *,
    name: Optional[str] = None,
    args: Optional[ParameterType] = None,
    returns: Optional[ReturnType] = None,
    with_null_masks: bool = False,
    function_type: str = 'udf',
) -> Callable[..., Any]:
    """Generic wrapper for UDF and TVF decorators."""

    _singlestoredb_attrs = {  # type: ignore
        k: v for k, v in dict(
            name=name,
            args=expand_types(args),
            returns=expand_types(returns),
            with_null_masks=with_null_masks,
            function_type=function_type,
        ).items() if v is not None
    }

    # No func was specified, this is an uncalled decorator that will get
    # called later, so the wrapper much be created with the func passed
    # in at that time.
    if func is None:
        def decorate(func: Callable[..., Any]) -> Callable[..., Any]:
            if with_null_masks:
                verify_masks(func)

            def wrapper(*args: Any, **kwargs: Any) -> Callable[..., Any]:
                return func(*args, **kwargs)  # type: ignore

            wrapper._singlestoredb_attrs = _singlestoredb_attrs  # type: ignore

            return functools.wraps(func)(wrapper)

        return decorate

    if with_null_masks:
        verify_masks(func)

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
) -> Callable[..., Any]:
    """
    Define a user-defined function (UDF).

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

    Returns
    -------
    Callable

    """
    return _func(
        func=func,
        name=name,
        args=args,
        returns=returns,
        with_null_masks=False,
        function_type='udf',
    )


def udf_with_null_masks(
    func: Optional[Callable[..., Any]] = None,
    *,
    name: Optional[str] = None,
    args: Optional[ParameterType] = None,
    returns: Optional[ReturnType] = None,
) -> Callable[..., Any]:
    """
    Define a user-defined function (UDF) with null masks.

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

    Returns
    -------
    Callable

    """
    return _func(
        func=func,
        name=name,
        args=args,
        returns=returns,
        with_null_masks=True,
        function_type='udf',
    )


def tvf(
    func: Optional[Callable[..., Any]] = None,
    *,
    name: Optional[str] = None,
    args: Optional[ParameterType] = None,
    returns: Optional[ReturnType] = None,
) -> Callable[..., Any]:
    """
    Define a table-valued function (TVF).

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

    Returns
    -------
    Callable

    """
    return _func(
        func=func,
        name=name,
        args=args,
        returns=returns,
        with_null_masks=False,
        function_type='tvf',
    )


def tvf_with_null_masks(
    func: Optional[Callable[..., Any]] = None,
    *,
    name: Optional[str] = None,
    args: Optional[ParameterType] = None,
    returns: Optional[ReturnType] = None,
) -> Callable[..., Any]:
    """
    Define a table-valued function (TVF) using null masks.

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

    Returns
    -------
    Callable

    """
    return _func(
        func=func,
        name=name,
        args=args,
        returns=returns,
        with_null_masks=True,
        function_type='tvf',
    )
