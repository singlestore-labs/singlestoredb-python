from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional

from vectorstore import AndFilter  # noqa: F401
from vectorstore import DeletionProtection  # noqa: F401
from vectorstore import EqFilter  # noqa: F401
from vectorstore import ExactMatchFilter  # noqa: F401
from vectorstore import FilterTypedDict  # noqa: F401
from vectorstore import GteFilter  # noqa: F401
from vectorstore import GtFilter  # noqa: F401
from vectorstore import IndexInterface  # noqa: F401
from vectorstore import IndexList  # noqa: F401
from vectorstore import IndexModel  # noqa: F401
from vectorstore import IndexStatsTypedDict  # noqa: F401
from vectorstore import InFilter  # noqa: F401
from vectorstore import LteFilter  # noqa: F401
from vectorstore import LtFilter  # noqa: F401
from vectorstore import MatchTypedDict  # noqa: F401
from vectorstore import Metric  # noqa: F401
from vectorstore import NamespaceStatsTypedDict  # noqa: F401
from vectorstore import NeFilter  # noqa: F401
from vectorstore import NinFilter  # noqa: F401
from vectorstore import OrFilter  # noqa: F401
from vectorstore import SimpleFilter  # noqa: F401
from vectorstore import Vector  # noqa: F401
from vectorstore import VectorDictMetadataValue  # noqa: F401
from vectorstore import VectorMetadataTypedDict  # noqa: F401
from vectorstore import VectorTuple  # noqa: F401
from vectorstore import VectorTupleWithMetadata  # noqa: F401


def vector_db(
    host: Optional[str] = None, user: Optional[str] = None,
    password: Optional[str] = None, port: Optional[int] = None,
    database: Optional[str] = None, driver: Optional[str] = None,
    pure_python: Optional[bool] = None, local_infile: Optional[bool] = None,
    charset: Optional[str] = None,
    ssl_key: Optional[str] = None, ssl_cert: Optional[str] = None,
    ssl_ca: Optional[str] = None, ssl_disabled: Optional[bool] = None,
    ssl_cipher: Optional[str] = None, ssl_verify_cert: Optional[bool] = None,
    tls_sni_servername: Optional[str] = None,
    ssl_verify_identity: Optional[bool] = None,
    conv: Optional[Dict[int, Callable[..., Any]]] = None,
    credential_type: Optional[str] = None,
    autocommit: Optional[bool] = None,
    results_type: Optional[str] = None,
    buffered: Optional[bool] = None,
    results_format: Optional[str] = None,
    program_name: Optional[str] = None,
    conn_attrs: Optional[Dict[str, str]] = {},
    multi_statements: Optional[bool] = None,
    client_found_rows: Optional[bool] = None,
    connect_timeout: Optional[int] = None,
    nan_as_null: Optional[bool] = None,
    inf_as_null: Optional[bool] = None,
    encoding_errors: Optional[str] = None,
    track_env: Optional[bool] = None,
    enable_extended_data_types: Optional[bool] = None,
    vector_data_format: Optional[str] = None,
    parse_json: Optional[bool] = None,
    pool_size: Optional[int] = 5,
    max_overflow: Optional[int] = 10,
    timeout: Optional[float] = 30,
) -> Any:
    """
    Return a vectorstore API connection.
    Database should be specified in the URL or as a keyword.

    Parameters
    ----------
    host : str, optional
        Hostname, IP address, or URL that describes the connection.
        The scheme or protocol defines which database connector to use.
        By default, the ``mysql`` scheme is used. To connect to the
        HTTP API, the scheme can be set to ``http`` or ``https``. The username,
        password, host, and port are specified as in a standard URL. The path
        indicates the database name. The overall form of the URL is:
        ``scheme://user:password@host:port/db_name``.  The scheme can
        typically be left off (unless you are using the HTTP API):
        ``user:password@host:port/db_name``.
    user : str, optional
        Database user name
    password : str, optional
        Database user password
    port : int, optional
        Database port. This defaults to 3306 for non-HTTP connections, 80
        for HTTP connections, and 443 for HTTPS connections.
    database : str, optional
        Database name.
    pure_python : bool, optional
        Use the connector in pure Python mode
    local_infile : bool, optional
        Allow local file uploads
    charset : str, optional
        Character set for string values
    ssl_key : str, optional
        File containing SSL key
    ssl_cert : str, optional
        File containing SSL certificate
    ssl_ca : str, optional
        File containing SSL certificate authority
    ssl_cipher : str, optional
        Sets the SSL cipher list
    ssl_disabled : bool, optional
        Disable SSL usage
    ssl_verify_cert : bool, optional
        Verify the server's certificate. This is automatically enabled if
        ``ssl_ca`` is also specified.
    ssl_verify_identity : bool, optional
        Verify the server's identity
    conv : dict[int, Callable], optional
        Dictionary of data conversion functions
    credential_type : str, optional
        Type of authentication to use: auth.PASSWORD, auth.JWT, or auth.BROWSER_SSO
    autocommit : bool, optional
        Enable autocommits
    results_type : str, optional
        The form of the query results: tuples, namedtuples, dicts,
        numpy, polars, pandas, arrow
    buffered : bool, optional
        Should the entire query result be buffered in memory? This is the default
        behavior which allows full cursor control of the result, but does consume
        more memory.
    results_format : str, optional
        Deprecated. This option has been renamed to results_type.
    program_name : str, optional
        Name of the program
    conn_attrs : dict, optional
        Additional connection attributes for telemetry. Example:
        {'program_version': "1.0.2", "_connector_name": "dbt connector"}
    multi_statements: bool, optional
        Should multiple statements be allowed within a single query?
    connect_timeout : int, optional
        The timeout for connecting to the database in seconds.
        (default: 10, min: 1, max: 31536000)
    nan_as_null : bool, optional
        Should NaN values be treated as NULLs when used in parameter
        substitutions including uploaded data?
    inf_as_null : bool, optional
        Should Inf values be treated as NULLs when used in parameter
        substitutions including uploaded data?
    encoding_errors : str, optional
        The error handler name for value decoding errors
    track_env : bool, optional
        Should the connection track the SINGLESTOREDB_URL environment variable?
    enable_extended_data_types : bool, optional
        Should extended data types (BSON, vector) be enabled?
    vector_data_format : str, optional
        Format for vector types: json or binary
    pool_size : int, optional
        The number of connections to keep in the connection pool. Default is 5.
    max_overflow : int, optional
        The maximum number of connections to allow beyond the pool size.
        Default is 10.
    timeout : float, optional
        The timeout for acquiring a connection from the pool in seconds.
        Default is 30 seconds.

    See Also
    --------
    :class:`Connection`

    Returns
    -------
    :class:`VectorDB`

    """
    from vectorstore import VectorDB
    return VectorDB(
        host=host, user=user, password=password, port=port,
        database=database, driver=driver, pure_python=pure_python,
        local_infile=local_infile, charset=charset,
        ssl_key=ssl_key, ssl_cert=ssl_cert, ssl_ca=ssl_ca,
        ssl_disabled=ssl_disabled, ssl_cipher=ssl_cipher,
        ssl_verify_cert=ssl_verify_cert,
        tls_sni_servername=tls_sni_servername,
        ssl_verify_identity=ssl_verify_identity, conv=conv,
        credential_type=credential_type, autocommit=autocommit,
        results_type=results_type, buffered=buffered,
        results_format=results_format, program_name=program_name,
        conn_attrs=conn_attrs, multi_statements=multi_statements,
        client_found_rows=client_found_rows,
        connect_timeout=connect_timeout, nan_as_null=nan_as_null,
        inf_as_null=inf_as_null, encoding_errors=encoding_errors,
        track_env=track_env,
        enable_extended_data_types=enable_extended_data_types,
        vector_data_format=vector_data_format,
        parse_json=parse_json, pool_size=pool_size,
        max_overflow=max_overflow, timeout=timeout,
    )
