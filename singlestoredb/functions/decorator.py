import functools
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from .dtypes import DataType


def listify(x: Any) -> List[Any]:
    """Make sure sure value is a list."""
    if x is None:
        return []
    if isinstance(x, (list, tuple, set)):
        return list(x)
    return [x]


def udf(
    func: Optional[Callable[..., Any]] = None,
    *,
    name: Optional[str] = None,
    database: Optional[str] = None,
    environment: Optional[str] = None,
    packages: Optional[Union[str, List[str]]] = None,
    resources: Optional[Union[str, List[str]]] = None,
    max_batch_size: int = 500,
    n_processes: int = 1,
    n_instances: int = 1,
    args: Optional[Union[DataType, List[DataType], Dict[str, DataType]]] = None,
    returns: Optional[str] = None,
    replace: bool = False,
    remote_service: Optional[str] = None,
    link: Optional[str] = None,
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
    database : str, optional
        The database to create the functions in
    environment : str, optional
        The environment to create for the functions
    packages : str or list-of-strs, optional
        The package dependency specifications. Strings must be in the
        format used in requirements.txt files.
    max_match_size : int, optional
        The number of rows to batch in the server before sending
        them to the UDF application
    n_processes : int, optional
        The number of sub-processes to spin up to process sub-batches
        of rows of data. This may be used if the UDF is CPU-intensive
        and there are free CPUs in the server the web application
        is running in. If the UDF is very short-running, setting this
        parameter to greater than one will likey cause the UDF to
        run slower since the overhead of the extra processes and moving
        data would be the limiting factor.
    n_instances : int, optional
        The number of runtime environments to use to handle data
        processing requests
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
    replace : bool, optional
        Should an existing function of the same name be replaced when
        creating the function in the database?
    remote_service : str, optional
        URL of the remote service that handles this function. If using an
        environment, this URL is generated automatically.
    link : str, optional
        Name of link to use to connect to remote service
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
    assert max_batch_size >= 1
    assert n_processes >= 1
    assert n_instances >= 1

    if args is None:
        pass
    elif isinstance(args, (list, tuple)):
        args = list(args)
        for i, item in enumerate(args):
            if callable(item):
                args[i] = item()
        for item in args:
            if not isinstance(item, str):
                raise TypeError(f'unrecognized type for parameter: {item}')
    elif isinstance(args, dict):
        args = dict(args)
        for k, v in list(args.items()):
            if callable(v):
                args[k] = v()
        for item in args.values():
            if not isinstance(item, str):
                raise TypeError(f'unrecognized type for parameter: {item}')
    elif callable(args):
        args = args()
    elif isinstance(args, str):
        args = args
    else:
        raise TypeError(f'unrecognized data type for args: {args}')

    if returns is None:
        pass
    elif callable(returns):
        returns = returns()
    elif isinstance(returns, str):
        returns = returns
    else:
        raise TypeError(f'unrecognized return type: {returns}')

    if returns is not None and not isinstance(returns, str):
        raise TypeError(f'unrecognized return type: {returns}')

    if include_masks and data_format == 'python':
        raise RuntimeError(
            'include_masks is only valid when using '
            'vectors for input parameters',
        )

    _singlestoredb_attrs = {  # type: ignore
        k: v for k, v in dict(
            name=name,
            database=database,
            environment=environment,
            packages=listify(packages),
            resources=listify(resources),
            max_batch_size=max(1, int(max_batch_size)),
            n_processes=max(1, int(n_processes)),
            n_instances=max(1, int(n_instances)),
            args=args,
            returns=returns,
            replace=bool(replace),
            remote_service=remote_service,
            link=link,
            data_format=data_format,
            include_masks=include_masks,
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


udf.pandas = functools.partial(udf, data_format='pandas')  # type: ignore
udf.polars = functools.partial(udf, data_format='polars')  # type: ignore
udf.arrow = functools.partial(udf, data_format='arrow')  # type: ignore
udf.numpy = functools.partial(udf, data_format='numpy')  # type: ignore
