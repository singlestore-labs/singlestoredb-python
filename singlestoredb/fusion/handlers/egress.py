#!/usr/bin/env python3
import datetime
import json
import sys
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from .. import result
from ...management.egress import Catalog
from ...management.egress import EgressService
from ...management.egress import Link
from ...management.egress import S3Link
from ...management.region import Region
from ..handler import SQLHandler
from ..result import FusionSQLResult
from .utils import get_workspace_group


# Placeholders overridden by use_db decorator
conn: Any = None
cur: Any = None
sqlite3: Any = None


def error(*args: Any, **kwargs: Any) -> None:
    """Print error message to stderr."""
    print('ERROR:', *args, file=sys.stderr, **kwargs)


def use_db(func: Any) -> Any:
    """Wrapper to inject database object."""

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        conn = None
        cur = None

        try:
            import sqlite3

            conn = sqlite3.connect('egress.db')
            cur = conn.cursor()
            cur.row_factory = None

            cur.execute(r'''
                CREATE TABLE IF NOT EXISTS catalogs(
                    name UNIQUE,
                    type,
                    protocol,
                    credentials,
                    config,
                    description
                )
            ''')

            cur.execute(r'''
                CREATE TABLE IF NOT EXISTS egresses(
                    name UNIQUE,
                    from_database,
                    from_table,
                    into_database,
                    into_table,
                    columns,
                    catalog,
                    storage_link,
                    storage_url,
                    properties,
                    order_by,
                    partition_by,
                    description
                )
            ''')

            cur.execute(r'''
                CREATE TABLE IF NOT EXISTS egress_run(
                    name UNIQUE,
                    start_time,
                    tags,
                    status
                )
            ''')

            func.__globals__['sqlite3'] = sqlite3
            func.__globals__['conn'] = conn
            func.__globals__['cur'] = cur

            return func(*args, **kwargs)

        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()

    return wrapper


@use_db
def create_external_catalog(
    name: str,
    catalog_type: str,
    protocol: str,
    credentials: Dict[str, Any],
    config: Dict[str, Any],
    description: str,
    if_not_exists: bool = False,
) -> None:
    """Create catalog."""
    try:
        cur.execute(
            'INSERT INTO catalogs VALUES (?, ?, ?, ?, ?, ?)',
            (
                name,
                catalog_type,
                protocol,
                json.dumps(credentials),
                json.dumps(config),
                description,
            ),
        )
        conn.commit()

    except sqlite3.IntegrityError:
        error(f'Catalog with name "{name}" already exists')


@use_db
def show_catalogs(like: Optional[str] = None) -> Tuple[Any, ...]:
    """Show catalogs."""
    query = 'SELECT name FROM catalogs'
    if like:
        cur.execute(query + ' WHERE name LIKE ?', (like,))
    else:
        cur.execute(query)
    return cur.fetchall()


@use_db
def describe_catalog(name: str) -> Tuple[Any, ...]:
    """Show catalogs."""
    query = 'SELECT name, type, protocol, config, description ' + \
            'FROM catalogs WHERE name = ?'
    cur.execute(query, (name,))
    return cur.fetchall()


@use_db
def drop_catalog(name: str, if_exists: bool = False) -> None:
    """Delete catalog."""
    if not if_exists:
        cur.execute('SELECT count(*) FROM catalogs WHERE name=?', (name,))
        if cur.fetchone()[0] == 0:
            error(f'No catalog with name "{name}" found')
            return

    cur.execute('DELETE FROM catalogs WHERE name = ?', (name,))
    conn.commit()


def _get_catalog(catalog: str) -> Catalog:
    cur.execute(
        'SELECT type, config FROM catalogs WHERE name=?',
        (catalog,),
    )
    catalogs = cur.fetchall()
    if len(catalogs) == 0:
        error(f'No catalog with name "{catalog}" found')

    # Catalog
    type, _config = catalogs[0][0]
    config = json.loads(_config or '{}')

    manager = get_workspace_group({})._manager
    if manager is None:
        raise ValueError('No workspace manager is available.')

    out = Catalog.from_dict(
        dict(
            catalogSource=type.upper(),
            tableFormat='ICEBERG',
            glueRegion=config.get('region'),
            glueCatalogID=config.get('catalog_id'),
        ),
        manager,
    )

    return out


def _get_link(link: str) -> Link:
    # TODO
    return S3Link(region=Region(id='xxx', name='us-east-1', provider='aws'))


def _get_egress_service(
    database: str,
    table: str,
    catalog: str,
    storage_link: str,
    storage_url: str,
) -> EgressService:
    """Return an egress service object for given parameters."""
    return EgressService(
        get_workspace_group({}),
        database,
        table,
        _get_catalog(catalog),
        _get_link(storage_link),
        storage_url,
        columns=None,
    )


@use_db
def create_cluster_identity(
    catalog: str,
    storage_url: str,
) -> Dict[str, Any]:
    """Create cluster identity."""
    return _get_egress_service(
        'dummy', 'dummy', catalog, 'dummy', storage_url,
    ).create_cluster_identity()


@use_db
def create_egress(
    name: str,
    from_database: str,
    from_table: str,
    into_database: str,
    into_table: str,
    columns: List[str],
    catalog: str,
    storage_link: str,
    storage_url: str,
    properties: Dict[str, Any],
    order_by: List[Dict[str, Any]],
    partition_by: List[Dict[str, Any]],
    description: str,
    if_not_exists: bool = False,
) -> None:
    """Create egress configuration."""
    if not if_not_exists:
        cur.execute('SELECT count(*) FROM egresses WHERE name=?', (name,))
        if cur.fetchone()[0] > 0:
            error(f'Egress already exists with name "{name}"')
            return

    # Make sure catalog profile exists
    cur.execute('SELECT count(*) FROM catalogs where name = ?', (catalog,))
    if cur.fetchall()[0][0] == 0:
        error(f'No catalog profile found with the name "{catalog}"')
        return

    # TODO: Look up link name

    cur.execute(
        r'''
            INSERT INTO egresses VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        ''',
        (
            name, from_database, from_table, into_database, into_table,
            json.dumps(columns), catalog, storage_link, storage_url,
            json.dumps(properties), json.dumps(order_by),
            json.dumps(partition_by), description,
        ),
    )
    conn.commit()


@use_db
def describe_egress(name: str, extended: bool = False) -> Tuple[Any, ...]:
    """Show egress configurations."""
    columns = 'egresses.name, from_database, from_table'
    if extended:
        columns += ', into_database, into_table'
    columns += ', columns, catalog, storage_link, storage_url'
    if extended:
        columns += ', properties, order_by, partition_by'
    columns += ', description, egress_run.status'

    cur.execute(
        f'SELECT {columns} FROM egresses LEFT JOIN egress_run '
        'WHERE egresses.name = ? and (egresses.name = egress_run.name or '
        'egress_run.name IS NULL)', (name,),
    )

    return cur.fetchall()


@use_db
def show_egresses(like: Optional[str] = None) -> Tuple[Any, ...]:
    """Show egress configurations."""
    cur.execute(
        'SELECT name FROM egresses WHERE name LIKE ?', (like or '%',),
    )
    return cur.fetchall()


@use_db
def start_egress(
    name: str,
    tags: List[str],
    if_not_running: bool = False,
) -> None:
    """Start an egress process."""
    cur.execute(
        'SELECT from_database, from_table, catalog, storage_link, storage_url ' +
        'FROM egresses WHERE name = ?',
        (name,),
    )
    out = cur.fetchall()
    if len(out) == 0:
        error(f'No egress found with name "{name}"')
        return

    database, table, catalog, storage_link, storage_url = out[0]

    if not if_not_running:
        cur.execute('SELECT status FROM egress_run WHERE name = ?', (name,))
        out = cur.fetchall()
        if out and out[0][0].lower() in ['Running', 'Stopped']:
            error(f'Egress with name "{name}" is already running')
            return

    cur.execute(
        'INSERT INTO egress_run VALUES (?, ?, ?, ?)',
        (name, datetime.datetime.now(), json.dumps(tags), 'Running'),
    )
    conn.commit()

    egress = _get_egress_service(database, table, catalog, storage_link, storage_url)

    egress.start(tags=tags)

    return None


@use_db
def stop_egress(name: str, if_running: bool = False) -> None:
    """Stop an egress process."""
    if not if_running:
        cur.execute('SELECT status FROM egress_run WHERE name = ?', (name,))
        out = cur.fetchall()
        if out and out[0][0].lower() not in ['Running', 'Stopped']:
            error(f'Egress with name "{name}" is not running')
            return

    cur.execute('SELECT count(*) FROM egress_run WHERE name = ?', (name,))
    if cur.fetchall()[0][0] == 0:
        error(f'No egress run found with name "{name}"')
        return

    cur.execute('UPDATE egress_run SET status = "Aborted" WHERE name = ?', (name,))
    conn.commit()


@use_db
def drop_egress(name: str, if_exists: bool = False) -> None:
    """Delete an egress configuration."""
    if not if_exists:
        cur.execute('SELECT count(*) FROM egresses WHERE name = ?', (name,))
        if cur.fetchall()[0][0] == 0:
            error(f'No egress found with name "{name}"')
            return

    cur.execute('DELETE FROM egresses WHERE name = ?', (name,))
    conn.commit()


class CreateExternalCatalog(SQLHandler):
    """
    CREATE EXTERNAL CATALOG [ if_not_exists ] name
        as_catalog_type
        [ credentials ]
        [ config ]
        [ description ]
    ;

    # If not exists
    if_not_exists = IF NOT EXISTS

    # Catalog name
    name = <catalog-name>

    # Catalog type
    as_catalog_type = AS { GLUE | HIVE | POLARIS | REST | SNOWFLAKE }

    # Credentials
    credentials = CREDENTIALS '<credentials-json>'

    # Config
    config = CONFIG '<config-json>'

    # Description
    description = DESCRIPTION '<description>'

    Description
    -----------
    Create a catalog profile.

    Arguments
    ---------
    * ``<catalog-name>``: Name of the catalog profile.
    * ``<credentials-json>``: JSON object containing credentials
    * ``<config-json>``: JSON object containing parameters for the
      given catalog type
    * ``<description>``: Description of catalog.

    Example
    -------
    The following command creates an Iceberg v2 catalog profile using AWS Glue
    named 'example-iceberg'::

        CREATE EXTERNAL CATALOG 'example-iceberg' AS GLUE
            CONFIG '{ "region": "us-east-1", "catalog_id": "a12347382" }';

    """
    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        create_external_catalog(
            params['name'],
            'ICEBERG',
            params['as_catalog_type'].upper(),
            params['credentials'],
            params['config'],
            params['description'],
            if_not_exists=params['if_not_exists'],
        )
        return None


CreateExternalCatalog.register(overwrite=True)


class ShowCatalogs(SQLHandler):
    """
    SHOW CATALOGS [ <like> ];

    Description
    -----------
    Show all catalog profiles.

    Example
    -------
    The following statement shows all catalog profiles with names beginning
    with "dev"::

        SHOW CATALOGS LIKE "dev%";

    """
    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        res = FusionSQLResult()
        res.add_field('Name', result.STRING)
        res.set_rows(show_catalogs(**params))
        return res


ShowCatalogs.register(overwrite=True)


class DescribeCatalog(SQLHandler):
    """
    DESCRIBE CATALOG name;

    name = <catalog-name>

    Description
    -----------
    Show information about catalog profile.

    Example
    -------
    The following command shows information about the catalog named "dev-cat"::

        DESCRIBE CATALOG dev-cat;

    """
    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        res = FusionSQLResult()
        res.add_field('Name', result.STRING)
        res.add_field('Type', result.STRING)
        res.add_field('Protocol', result.STRING)
        res.add_field('Config', result.JSON)
        res.set_rows(describe_catalog(**params))
        return res


DescribeCatalog.register(overwrite=True)


class DropCatalog(SQLHandler):
    """
    DROP CATALOG [ if_exists ] name;

    # If catalog exists
    if_exists = IF EXISTS

    # Catalog name
    name = <catalog-name>

    Description
    -----------
    Delete a catalog profile.

    Arguments
    ---------
    * ``<catalog-name>``: Name of the catalog profile to delete.

    Example
    -------
    The following statement deletes a catalog profile with the name "dev-cat"::

        DROP CATALOG dev-cat;

    """
    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        drop_catalog(**params)
        return None


DropCatalog.register(overwrite=True)


class CreateClusterIdentity(SQLHandler):
    """
    CREATE CLUSTER IDENTITY using_catalog with_storage_base_url;

    # Using catalog
    using_catalog = USING CATALOG <catalog-name>

    # Storage URL
    with_storage_base_url = WITH STORAGE BASE URL '<storage-url>'

    Description
    -----------
    Create a cluster identity for allowing the egress service to access
    external cloud resources.

    Example
    -------
    * ``<catalog-name>``: Name of the catalog profile to use.
    * ``<storage-url>``: URL of the data storage.

    Example
    -------
    The following statement creates a cluster identity for the catalog
    profile name "dev-cat" and data storage URL "s3://bucket-name/data"::

        CREATE CLUSTER IDENTITY
            USING CATALOG dev-cat
            WITH STORAGE BASE URL 's3://bucket-name/data';

    """
    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        print(
            create_cluster_identity(
                params['using_catalog'],
                params['with_storage_base_url'],
            ),
        )
        return None


CreateClusterIdentity.register(overwrite=True)


class CreateEgress(SQLHandler):
    """
    CREATE EGRESS [ if_not_exists ] name
        from_table [ colnames ]
        [ into_table ]
        catalog
        storage
        [ properties ]
        [ ordered_by ]
        [ partitioned_by ]
        [ description ]
    ;

    # If not exists
    if_not_exists = IF NOT EXISTS

    # Egress name
    name = '<egress-name>'

    # From table
    from_table = FROM <table>

    # Into table
    into_table = INTO <table>

    # Column names
    colnames = ( <column>,... )

    # Properties
    properties = PROPERTIES '<table-properties>'

    # Catolog profile
    catalog = CATALOG <catalog-name>

    # Storage profile
    storage = _storage_link _storage_base_url
    _storage_link = LINK <link-name>
    _storage_base_url = '<storage-base-url>'

    # Transforms
    __transform = {
    &&&    __identity_func |
    &&&    __bucket_func |
    &&&    __truncate_func |
    &&&    __year_func |
    &&&    __month_func |
    &&&    __day_func |
    &&&    __hour_func |
    &&&    __void_func
    && }
    __identity_func = IDENTITY( __col_param )
    __bucket_func = BUCKET( __col_param comma __bucket_n )
    __truncate_func = TRUNCATE( __col_param comma __truncate_n )
    __year_func = YEAR( __col_param )
    __month_func = MONTH( __col_param )
    __day_func = DAY( __col_param )
    __hour_func = HOUR( __col_param )
    __void_func = VOID( __col_param )
    __bucket_n = <integer>
    __truncate_n = <integer>
    __col_param = <column>

    # Order by
    ordered_by = ORDER BY _order_specs

    _order_specs = _order_spec,...
    _order_spec = {
    &&    __transform |  __order_col
    & }
    & [ __order_direction ]
    & [ __order_nulls ]
    __order_direction = { ASC | DESC }
    __order_nulls = NULLS { FIRST | LAST }
    __order_col = <column>

    # Partitioning
    partitioned_by = PARTITION BY _partition_spec,...
    _partition_spec = {
    &&    __transform | __partition_col
    & }
    __partition_col = <column>

    description = DESCRIPTION '<description>'

    Description
    -----------
    Create an egress configuration.

    Arguments
    ---------
    * ``<egress-name>``: The name to give the egress configuration.
    * ``<column>``: Name of a column in the source table.
    * ``<catalog-name>``: Name of a catalog profile.
    * ``<link-name>``: Name of the link for accessing data storage.
    * ``<table-properties>``: Table properties as a JSON object.
    * ``<storage-base-url>``: Base URL of data storage.
    * ``<region-name>``: Name of region for storage location.
    * ``<description>``: Description of egress.

    Remarks
    -------
    * ``IF NOT EXISTS`` indicates that the egress configuration should only be
      created if there isn't one with the given name.
    * ``FROM <table>`` specifies the SingleStore table to egress. The same name will
      be used for the egressed table.
    * ``INTO <table>`` specifies the external table to write to. If this is not
      specified, the source database and table name are used.
    * ``CATALOG`` specifies the name of a catalog profile.
    * ``LINK`` indicates the name of a link for accessing storage.
    * ``ORDER BY`` indicates the sort order for the rows in the data files.
      ``ASC`` and ``DESC`` can be used to indicate ascending or descending order.
      ``NULLS FIRST`` and ``NULLS LAST`` can be used to indicate where nulls
      should occur in the sort order.
    * ``PARTITION BY`` indicates how the data files should be partitioned.

    Examples
    --------
    The following statement creates an egress configuration named "dev-egress" using
    catalog profile named "dev-cat" and link named "dev-link" with storage base URL
    of "s3://bucket-name/data".
    The source table to egress is named "customer_data"::

        CREATE EGRESS dev-egress FROM customer_data
            CATALOG dev-cat
            LINK dev-link "s3://bucket-name/data"
            PROPERTIES '{
                "write.update.mode": "copy-on-write",
                "write.format.default": "parquet",
                "write.parquet.row-group-size-bytes": 50000000,
                "write.target-file-size-bytes": 100000000
            }';

    The following statement adds ordering and partitioning to the above configuration::

        CREATE EGRESS dev-egress FROM customer_data
            CATALOG dev-cat
            LINK dev-link "s3://bucket-name/data"
            PROPERTIES '{
                "write.update.mode": "copy-on-write",
                "write.format.default": "parquet",
                "write.parquet.row-group-size-bytes": 50000000,
                "write.target-file-size-bytes": 100000000
            }'
            ORDER BY customer_id
            PARTITION BY last_name, country;

    """  # noqa

    def _process_parquet_options(self, opts: Dict[str, Any]) -> Dict[str, Any]:
        units = {'mb': int(1e6), 'kb': int(1e3)}
        out: Dict[str, Any] = {}
        if not opts:
            return out
        for k, v in opts.items():
            if k in ['page_size', 'target_file_size', 'row_group_size', 'dict_page_size']:
                out[k] = v[0] * units[v[1].lower()]
            else:
                out[k] = v
        return out

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        # Name
        if_not_exists = params['if_not_exists']
        name = params['name']

        # From table
        if len(params['from_table']) > 1:
            from_database, from_table = params['from_table']
        else:
            from_database = None
            from_table = params['from_table']

        # Into table
        if params['into_table'] is None:
            into_database = None
            into_table = None
        elif len(params['into_table']) > 1:
            into_database, into_table = params['into_table']
        else:
            into_database = None
            into_table = params['into_table']

        # Columns
        columns = params['colnames']

        # Catalog
        catalog = params['catalog']

        # Storage
        link = params['storage']['storage_link']
        base_url = params['storage']['storage_base_url']

        # Properties
        properties = json.loads(params['properties'] or '{}')

        # Order by
        order_by = []
        for spec in [x['order_spec'] for x in params['ordered_by']['order_specs']]:
            nulls = {'first': 'nulls_first', 'last': 'nulls_last'}
            if 'transform' in spec:
                order_by.append({
                    'name': list(spec['transform'].values())[0]['col_param'],
                    'direction': spec.get('order_direction', 'asc').lower(),
                    'null_order': nulls[spec.get('order_nulls', 'first').lower()],
                    'transform':
                    list(spec['transform'].keys())[0].lower().replace('_func', ''),
                })

            else:
                order_by.append({
                    'name': spec['order_col'],
                    'direction': spec.get('order_direction', 'asc').lower(),
                    'null_order': nulls[spec.get('order_nulls', 'first').lower()],
                    'transform': 'identity',
                })

        # Partition by
        partition_by = []
        for spec in [x['partition_spec'] for x in params['partitioned_by']]:
            if 'transform' in spec:
                partition_by.append({
                    'name': list(spec['transform'].values())[0]['col_param'],
                    'transform':
                    list(spec['transform'].keys())[0].lower().replace('_func', ''),
                })

            else:
                partition_by.append({
                    'name': spec['partition_col'],
                    'transform': 'identity',
                })

        create_egress(
            name,
            from_database,
            from_table,
            into_database,
            into_table,
            columns,
            catalog,
            link,
            base_url,
            properties,
            order_by,
            partition_by,
            params['description'],
            if_not_exists=if_not_exists,
        )

        return None


CreateEgress.register(overwrite=True)


class ShowEgresses(SQLHandler):
    """
    SHOW EGRESSES [ <like> ];

    Description
    -----------
    Show egress configurations.

    Examples
    --------
    The following statement shows all egress configurations starting with "dev"::

        SHOW EGRESSES LIKE "dev%";

    """
    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        res = FusionSQLResult()
        res.add_field('Name', result.STRING)
        res.set_rows(show_egresses(**params))
        return res


ShowEgresses.register(overwrite=True)


class DescribeEgress(SQLHandler):
    """
    DESCRIBE EGRESS name [ extended ];

    # Egress config name
    name = '<egress-name>'

    # Show more details
    extended = EXTENDED

    Description
    -----------
    Show egress configuration for given name.

    Arguments
    ---------
    * ``<egress-name>``: Name of the egress configuration.

    Examples
    --------
    The following statement shows the egress configuration named "dev-egress"::

        DESCRIBE EGRESS dev-egress;

    """
    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        res = FusionSQLResult()

        res.add_field('Name', result.STRING)
        res.add_field('FromDatabase', result.STRING)
        res.add_field('FromTable', result.STRING)

        if params['extended']:
            res.add_field('IntoDatabase', result.STRING)
            res.add_field('IntoTable', result.STRING)

        res.add_field('Columns', result.JSON)
        res.add_field('Catalog', result.STRING)
        res.add_field('StorageLink', result.STRING)
        res.add_field('StorageBaseURL', result.STRING)

        if params['extended']:
            res.add_field('Properties', result.JSON)
            res.add_field('OrderBy', result.JSON)
            res.add_field('PartitionBy', result.JSON)

        res.add_field('Description', result.STRING)
        res.add_field('Status', result.STRING)

        res.set_rows(describe_egress(**params))

        return res


DescribeEgress.register(overwrite=True)


class StopEgress(SQLHandler):
    """
    STOP EGRESS name [ if_running ];

    # Egress config name
    name = '<egress-name>'

    # Do not return error if already running
    if_running = IF RUNNING

    Description
    -----------
    Stop a running egress process.

    Arguments
    ---------
    * ``<egress-name>``: Name of the egress configuration.

    Examples
    --------
    The following statement stops an egress process using the configuration
    named "dev-egress"::

        STOP EGRESS dev-egress;

    """
    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        stop_egress(**params)
        return None


StopEgress.register(overwrite=True)


class StartEgress(SQLHandler):
    """
    START EGRESS name [ if_not_running ] [ with_tag ];

    # Egress config name
    name = '<egress-name>'

    # Do not return error if already running
    if_not_running = IF NOT RUNNING

    # Iceberg tags
    with_tag = WITH TAG '<tag>',...

    Description
    -----------
    Start an egress process using the given configuration name.

    Arguments
    ---------
    * ``<egress-name>``: Name of egress configuration to use.

    Examples
    --------
    The following statement starts an egress process using the configuration
    named "dev-egress"::

        START EGRESS dev-egress;

    """
    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        params['tags'] = params.pop('with_tag') or []
        start_egress(**params)
        return None


StartEgress.register(overwrite=True)


class DropEgress(SQLHandler):
    """
    DROP EGRESS [ if_exists ] name;

    # Do not return error if egress doesn't exist
    if_exists = IF EXISTS

    # Egress config name
    name = '<egress-name>'

    Description
    -----------
    Delete an egress configuration.

    Arguments
    ---------
    * ``<egress-name>``: Name of the egress configuration.

    Examples
    --------
    The following statement deletes an egress configuration named "dev-egress"::

        DROP EGRESS dev-egress;

    """
    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        drop_egress(**params)
        return None


DropEgress.register(overwrite=True)
