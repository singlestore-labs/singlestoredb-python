#!/usr/bin/env python
"""SingleStoreDB package options."""
import functools
import os

from . import auth
from .utils.config import check_bool  # noqa: F401
from .utils.config import check_dict_str_str  # noqa: F401
from .utils.config import check_float  # noqa: F401
from .utils.config import check_int  # noqa: F401
from .utils.config import check_optional_bool  # noqa: F401
from .utils.config import check_str  # noqa: F401
from .utils.config import check_url  # noqa: F401
from .utils.config import describe_option  # noqa: F401
from .utils.config import get_default  # noqa: F401
from .utils.config import get_option  # noqa: F401
from .utils.config import get_suboptions  # noqa: F401
from .utils.config import option_context  # noqa: F401
from .utils.config import options  # noqa: F401
from .utils.config import register_option  # noqa: F401
from .utils.config import reset_option  # noqa: F401
from .utils.config import set_option  # noqa: F401


#
# Connection options
#
register_option(
    'host', 'string', check_str, '127.0.0.1',
    'Specifies the database host name or IP address.',
    environ=['SINGLESTOREDB_HOST', 'SINGLESTOREDB_URL'],
)

register_option(
    'port', 'int', check_int, 0,
    'Specifies the database port number.',
    environ='SINGLESTOREDB_PORT',
)

register_option(
    'http_port', 'int', check_int, 0,
    'Specifies the database port number for the HTTP API.',
    environ='SINGLESTOREDB_HTTP_PORT',
)

register_option(
    'user', 'string', check_str, None,
    'Specifies the database user name.',
    environ='SINGLESTOREDB_USER',
)

register_option(
    'password', 'string', check_str, None,
    'Specifies the database user password.',
    environ='SINGLESTOREDB_PASSWORD',
)

register_option(
    'driver', 'string', check_str, 'mysql',
    'Specifies the Python DB-API module to use for communicating'
    'with the database.',
    environ='SINGLESTOREDB_DRIVER',
)

register_option(
    'database', 'string', check_str, None,
    'Name of the database to connect to.',
    environ='SINGLESTOREDB_DATABASE',
)

register_option(
    'pure_python', 'bool', check_optional_bool, None,
    'Should the driver use a pure Python implementation? If the value is '
    '`None`, the C extension will be used if it exists, and pure python '
    'will be used otherwise. If the value is `False`, the pure python '
    'implementation will be used. If the value is `True` and the C extension '
    'exists, it will be used. If the value is `True` and the C extension '
    'doesn\'t exist or can\'t be loaded, a `NotSupportedError` is raised.',
    environ='SINGLESTOREDB_PURE_PYTHON',
)

register_option(
    'charset', 'string', check_str, 'utf8mb4',
    'Specifies the character set for the session.',
    environ='SINGLESTOREDB_CHARSET',
)

register_option(
    'encoding_errors', 'string', check_str, 'strict',
    'Specifies the error handling behavior for decoding string values.',
    environ='SINGLESTOREDB_ENCODING_ERRORS',
)

register_option(
    'local_infile', 'bool', check_bool, False,
    'Should it be possible to load local files?',
    environ='SINGLESTOREDB_LOCAL_INFILE',
)

register_option(
    'multi_statements', 'bool', check_bool, False,
    'Should it be possible use multiple statements in one query?',
    environ='SINGLESTOREDB_MULTI_STATEMENTS',
)

register_option(
    'client_found_rows', 'bool', check_bool, False,
    'Should affected_rows in OK_PACKET indicate the '
    'number of matched rows instead of changed?',
    environ='SINGLESTOREDB_CLIENT_FOUND_ROWS',
)

register_option(
    'ssl_key', 'str', check_str, None,
    'File containing SSL key',
    environ='SINGLESTOREDB_SSL_KEY',
)

register_option(
    'ssl_cert', 'str', check_str, None,
    'File containing SSL certificate',
    environ='SINGLESTOREDB_SSL_CERT',
)

register_option(
    'ssl_ca', 'str', check_str, None,
    'File containing SSL certificate authority',
    environ='SINGLESTOREDB_SSL_CA',
)

register_option(
    'ssl_cipher', 'str', check_str, 'HIGH',
    'Sets the SSL cipher list',
    environ='SINGLESTOREDB_SSL_CIPHER',
)

register_option(
    'tls_sni_servername', 'str', check_str, None,
    'Sets TLS SNI servername',
    environ='SINGLESTOREDB_TLS_SNI_SERVERNAME',
)

register_option(
    'ssl_disabled', 'bool', check_bool, False,
    'Disable SSL usage',
    environ='SINGLESTOREDB_SSL_DISABLED',
)

register_option(
    'ssl_verify_cert', 'bool', check_optional_bool, None,
    'Verify the server\'s certificate',
    environ='SINGLESTOREDB_SSL_VERIFY_CERT',
)

register_option(
    'ssl_verify_identity', 'bool', check_optional_bool, None,
    'Verify the server\'s identity',
    environ='SINGLESTOREDB_SSL_VERIFY_IDENTITY',
)

register_option(
    'program_name', 'string', check_str, None,
    'Name of the program',
)

register_option(
    'conn_attrs', 'dict', check_dict_str_str, None,
    'Additional connection attributes for telemetry',
)

register_option(
    'credential_type', 'str',
    functools.partial(
        check_str, valid_values=[
            auth.PASSWORD,
            auth.JWT,
            auth.BROWSER_SSO,
        ],
    ),
    None,
    'Type of authentication method to use.',
    environ='SINGLESTOREDB_CREDENTIAL_TYPE',
)

register_option(
    'sso_browser', 'str', check_str, None,
    'Browser to use for single sign-on. This should be a web browser name '
    'registered with Python\'s webbrowser module.',
    environ='SINGLESTOREDB_SSO_BROWSER',
)

register_option(
    'autocommit', 'bool', check_bool, True,
    'Enable autocommits',
    environ='SINGLESTOREDB_AUTOCOMMIT',
)

register_option(
    'buffered', 'bool', check_bool, True,
    'Should query results be buffered before processing?',
    environ='SINGLESTOREDB_BUFFERED',
)

register_option(
    'parse_json', 'bool', check_bool, True,
    'Parse JSON values into Python objects?',
    environ='SINGLESTOREDB_PARSE_JSON',
)

register_option(
    'connect_timeout', 'int', check_int, 10,
    'The timeout for connecting to the database in seconds. '
    '(default: 10, min: 1, max: 31536000)',
    environ='SINGLESTOREDB_CONNECT_TIMEOUT',
)

register_option(
    'nan_as_null', 'bool', check_bool, False,
    'Should NaN values be treated as NULLs in query parameter substitutions '
    'including uploaded data?',
    environ='SINGLESTOREDB_NAN_AS_NULL',
)

register_option(
    'inf_as_null', 'bool', check_bool, False,
    'Should Inf values be treated as NULLs in query parameter substitutions '
    'including uploaded data?',
    environ='SINGLESTOREDB_INF_AS_NULL',
)

register_option(
    'track_env', 'bool', check_bool, False,
    'Should connections track the SINGLESTOREDB_URL environment variable?',
    environ='SINGLESTOREDB_TRACK_ENV',
)

register_option(
    'enable_extended_data_types', 'bool', check_bool, True,
    'Should extended data types (BSON, vector) be enabled?',
    environ='SINGLESTOREDB_ENABLE_EXTENDED_DATA_TYPES',
)

register_option(
    'vector_data_format', 'string',
    functools.partial(
        check_str,
        valid_values=['json', 'binary'],
    ),
    'binary',
    'Format for vector data values',
    environ='SINGLESTOREDB_VECTOR_DATA_FORMAT',
)

register_option(
    'fusion.enabled', 'bool', check_bool, False,
    'Should Fusion SQL queries be enabled?',
    environ='SINGLESTOREDB_FUSION_ENABLED',
)

#
# Query results options
#
register_option(
    'results.type', 'string',
    functools.partial(
        check_str,
        valid_values=[
            'tuple', 'tuples', 'namedtuple', 'namedtuples',
            'dict', 'dicts', 'structsequence', 'structsequences',
            'numpy', 'pandas', 'polars', 'arrow', 'pyarrow',
        ],
    ),
    'tuples',
    'What form should the query results take?',
    environ='SINGLESTOREDB_RESULTS_TYPE',
)

register_option(
    'results.arraysize', 'int', check_int, 1,
    'Number of result rows to download in `fetchmany` calls.',
    environ='SINGLESTOREDB_RESULTS_ARRAYSIZE',
)


#
# Workspace manager options
#
register_option(
    'management.token', 'string', check_str, None,
    'Specifies the authentication token for the management API.',
    environ=['SINGLESTOREDB_MANAGEMENT_TOKEN'],
)

register_option(
    'management.base_url', 'string', check_str, 'https://api.singlestore.com',
    'Specifies the base URL for the management API.',
    environ=['SINGLESTOREDB_MANAGEMENT_BASE_URL'],
)

register_option(
    'management.version', 'string', check_str, 'v1',
    'Specifies the version for the management API.',
    environ=['SINGLESTOREDB_MANAGEMENT_VERSION'],
)


#
# External function options
#
register_option(
    'external_function.url', 'string', check_str, 'http://localhost:8000/invoke',
    'Specifies the URL of the external function application.',
    environ=['SINGLESTOREDB_EXT_FUNC_URL'],
)

register_option(
    'external_function.app_mode', 'string',
    functools.partial(
        check_str,
        valid_values=['remote', 'collocated', 'managed'],
    ),
    'remote',
    'Specifies the mode of operation of the external function application.',
    environ=['SINGLESTOREDB_EXT_FUNC_APP_MODE'],
)

register_option(
    'external_function.data_format', 'string',
    functools.partial(
        check_str,
        valid_values=['rowdat_1', 'json'],
    ),
    'rowdat_1',
    'Specifies the format for the data rows.',
    environ=['SINGLESTOREDB_EXT_FUNC_DATA_FORMAT'],
)

register_option(
    'external_function.data_version', 'string', check_str, '1.0',
    'Specifies the version of the data format.',
    environ=['SINGLESTOREDB_EXT_FUNC_DATA_VERSION'],
)

register_option(
    'external_function.link_name', 'string', check_str, None,
    'Specifies the link name to use for remote external functions.',
    environ=['SINGLESTOREDB_EXT_FUNC_LINK_NAME'],
)

register_option(
    'external_function.link_config', 'string', check_str, None,
    'Specifies the link config in JSON format.',
    environ=['SINGLESTOREDB_EXT_FUNC_LINK_CONFIG'],
)

register_option(
    'external_function.link_credentials', 'string', check_str, None,
    'Specifies the link credentials in JSON format.',
    environ=['SINGLESTOREDB_EXT_FUNC_LINK_CREDENTIALS'],
)

register_option(
    'external_function.replace_existing', 'bool', check_bool, False,
    'Should existing functions be replaced when registering external functions?',
    environ=['SINGLESTOREDB_EXT_FUNC_REPLACE_EXISTING'],
)

register_option(
    'external_function.socket_path', 'string', check_str, None,
    'Specifies the socket path for collocated external functions.',
    environ=['SINGLESTOREDB_EXT_FUNC_SOCKET_PATH'],
)

register_option(
    'external_function.max_connections', 'int', check_int, 32,
    'Specifies the maximum connections in a collocated external function ' +
    'before reusing them.',
    environ=['SINGLESTOREDB_EXT_FUNC_MAX_CONNECTIONS'],
)

register_option(
    'external_function.process_mode', 'string',
    functools.partial(
        check_str,
        valid_values=['thread', 'subprocess'],
    ),
    'subprocess',
    'Specifies the method to use for concurrent handlers in ' +
    'collocated external functions',
    environ=['SINGLESTOREDB_EXT_FUNC_PROCESS_MODE'],
)

register_option(
    'external_function.single_thread', 'bool', check_bool, False,
    'Should the collocated server run in single-thread mode?',
    environ=['SINGLESTOREDB_EXT_FUNC_SINGLE_THREAD'],
)

register_option(
    'external_function.log_level', 'string',
    functools.partial(
        check_str,
        valid_values=['info', 'debug', 'warning', 'error'],
    ),
    'info',
    'Logging level of external function server.',
    environ=['SINGLESTOREDB_EXT_FUNC_LOG_LEVEL'],
)

register_option(
    'external_function.name_prefix', 'string', check_str, '',
    'Prefix to add to external function names.',
    environ=['SINGLESTOREDB_EXT_FUNC_NAME_PREFIX'],
)

register_option(
    'external_function.name_suffix', 'string', check_str, '',
    'Suffix to add to external function names.',
    environ=['SINGLESTOREDB_EXT_FUNC_NAME_SUFFIX'],
)

register_option(
    'external_function.function_database', 'string', check_str, '',
    'Database to use for the function definitions.',
    environ=['SINGLESTOREDB_EXT_FUNC_FUNCTION_DATABASE'],
)

register_option(
    'external_function.connection', 'string', check_str,
    os.environ.get('SINGLESTOREDB_URL') or None,
    'Specifies the connection string for the database to register functions with.',
    environ=['SINGLESTOREDB_EXT_FUNC_CONNECTION'],
)

register_option(
    'external_function.host', 'string', check_str, 'localhost',
    'Specifies the host to bind the server to.',
    environ=['SINGLESTOREDB_EXT_FUNC_HOST'],
)

register_option(
    'external_function.port', 'int', check_int, 8000,
    'Specifies the port to bind the server to.',
    environ=['SINGLESTOREDB_EXT_FUNC_PORT'],
)

register_option(
    'external_function.timeout', 'int', check_int, 24*60*60,
    'Specifies the timeout in seconds for processing a batch of rows.',
    environ=['SINGLESTOREDB_EXT_FUNC_TIMEOUT'],
)

#
# Debugging options
#
register_option(
    'debug.queries', 'bool', check_bool, False,
    'Print queries and parameters to stderr.',
    environ='SINGLESTOREDB_DEBUG_QUERIES',
)

register_option(
    'debug.connection', 'bool', check_bool, False,
    'Print connection tracing information.',
    environ='SINGLESTOREDB_DEBUG_CONNECTION',
)
