import functools
import inspect
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

    returns = utils.get_annotations(obj).get('return', None)

    if inspect.isclass(returns) and issubclass(returns, str):
        return True

    raise TypeError(
        f'callable {obj} must return a str, '
        f'but got {returns}',
    )


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
) -> Callable[..., Any]:
    """Generic wrapper for UDF and TVF decorators."""

    _singlestoredb_attrs = {  # type: ignore
        k: v for k, v in dict(
            name=name,
            args=expand_types(args),
            returns=expand_types(returns),
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
) -> Callable[..., Any]:
    """
    Define a user-defined function (UDF).

    Parameters
    ----------
    func : callable, optional
        The UDF to apply parameters to
    name : str, optional
        The name to use for the UDF in the database
    args : str | Type | Callable | List[str | Callable], optional
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
    returns : str | Type | Callable | List[str | Callable] | Table, optional
        Specifies the return data type of the function. This parameter
        works the same way as `args`. If the function is a table-valued
        function, the return type should be a `Table` object.

    Returns
    -------
    Callable

    """
    return _func(
        func=func,
        name=name,
        args=args,
        returns=returns,
    )
