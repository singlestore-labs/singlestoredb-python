#!/usr/bin/env python3
import datetime
import json
import sys
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from singlestoredb.fusion import result
from singlestoredb.fusion.handler import SQLHandler
from singlestoredb.fusion.result import FusionSQLResult


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
                    parameters,
                    table_format,
                    table_format_parameters
                )
            ''')

            cur.execute(r'''
                CREATE TABLE IF NOT EXISTS egresses(
                    name UNIQUE,
                    database,
                    table_name,
                    columns,
                    catalog,
                    storage_base_url,
                    storage_region,
                    file_format,
                    file_format_parameters,
                    update_mode,
                    order_by,
                    partition_by
                )
            ''')

            cur.execute(r'''
                CREATE TABLE IF NOT EXISTS egress_run(
                    name UNIQUE,
                    start_time,
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
def create_catalog(
    name: str,
    catalog_type: str,
    catalog_params: Dict[str, Any],
    table_format: str,
    table_format_parameters: Dict[str, Any],
    if_not_exists: bool = False,
) -> None:
    try:
        cur.execute(
            'INSERT INTO catalogs VALUES (?, ?, ?, ?, ?)',
            (
                name, catalog_type, json.dumps(catalog_params),
                table_format, json.dumps(table_format_parameters),
            ),
        )
        conn.commit()

    except sqlite3.IntegrityError:
        error(f'Catalog configuration with name "{name}" already exists')


@use_db
def show_catalogs(like: Optional[str] = None, extended: bool = False) -> Tuple[Any, ...]:
    if extended:
        query = 'SELECT * FROM catalogs'
    else:
        query = 'SELECT name, type, table_format FROM catalogs'

    if like:
        cur.execute(query + ' WHERE name LIKE ?', (like,))
    else:
        cur.execute(query)

    return cur.fetchall()


@use_db
def drop_catalog(name: str, if_exists: bool = False) -> None:
    if not if_exists:
        cur.execute('SELECT count(*) FROM catalogs WHERE name=?', (name,))
        if cur.fetchone()[0] == 0:
            error(f'No catalog with name "{name}" found')
            return
    cur.execute('DELETE FROM catalogs WHERE name=?', (name,))
    conn.commit()


@use_db
def create_cluster_identity(catalog: str, storage_base_url: str) -> Dict[str, Any]:
    cur.execute('SELECT count(*) FROM catalogs WHERE name=?', (catalog,))
    if cur.fetchone()[0] == 0:
        error(f'No catalog with name "{catalog}" found')
    return {}


@use_db
def create_egress(
    name: str,
    database: str,
    table: str,
    columns: List[str],
    catalog: str,
    storage_base_url: str,
    storage_region: str,
    file_format: str,
    file_format_params: Dict[str, Any],
    update_mode: str,
    order_by: List[Dict[str, Any]],
    partition_by: List[Dict[str, Any]],
    if_not_exists: bool = False,
) -> None:
    if not if_not_exists:
        cur.execute('SELECT count(*) FROM egresses WHERE name=?', (name,))
        if cur.fetchone()[0] > 0:
            error(f'Egress already exists with name "{name}"')
            return

    # Make sure catalog exists
    cur.execute('SELECT count(*) FROM catalogs where name = ?', (catalog,))
    if cur.fetchall()[0][0] == 0:
        error(f'No catalog found with the name "{catalog}"')
        return

    cur.execute(
        r'''
            INSERT INTO egresses VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        ''',
        (
            name, database, table, json.dumps(columns), catalog,
            storage_base_url, storage_region, file_format, json.dumps(file_format_params),
            update_mode, json.dumps(order_by), json.dumps(partition_by),
        ),
    )
    conn.commit()


@use_db
def show_egress(name: str, extended: bool = False) -> None:
    columns = 'egresses.name, database, table_name, columns, catalog, storage_base_url'
    if extended:
        columns += ', storage_region, file_format, file_format_parameters, ' + \
                   'update_mode, order_by, partition_by'
    columns += ', egress_run.status'

    cur.execute(
        f'SELECT {columns} FROM egresses LEFT JOIN egress_run '
        'WHERE egresses.name = ? and egresses.name = egress_run.name or '
        'egress_run.name IS NULL', (name,),
    )

    return cur.fetchall()


@use_db
def show_egresses(like: Optional[str] = None, extended: bool = False) -> Tuple[Any, ...]:
    if like is None:
        like = '%'

    columns = 'egresses.name, database, table_name, columns, catalog, storage_base_url'
    if extended:
        columns += ', storage_region, file_format, file_format_parameters, ' + \
                   'update_mode, order_by, partition_by'
    columns += ', egress_run.status'

    cur.execute(
        f'SELECT {columns} FROM egresses LEFT JOIN egress_run '
        'WHERE egresses.name LIKE ? and egresses.name = egress_run.name or '
        'egress_run.name IS NULL', (like,),
    )

    return cur.fetchall()


@use_db
def start_egress(name: str, if_not_running: bool = False) -> None:
    cur.execute('SELECT count(*) FROM egresses WHERE name = ?', (name,))
    if cur.fetchall()[0][0] == 0:
        error(f'No egress found with name "{name}"')
        return

    if not if_not_running:
        cur.execute('SELECT status FROM egress_run WHERE name = ?', (name,))
        out = cur.fetchall()
        if out and out[0][0].lower() in ['running', 'pending']:
            error(f'Egress with name "{name}" is already running')
            return

    cur.execute(
        'INSERT INTO egress_run VALUES (?, ?, ?)',
        (name, datetime.datetime.now(), 'running'),
    )
    conn.commit()


@use_db
def stop_egress(name: str, if_running: bool = False) -> None:
    if not if_running:
        cur.execute('SELECT status FROM egress_run WHERE name = ?', (name,))
        out = cur.fetchall()
        if out and out[0][0].lower() not in ['running', 'pending']:
            error(f'Egress with name "{name}" is not running')
            return

    cur.execute('SELECT count(*) FROM egress_run WHERE name = ?', (name,))
    if cur.fetchall()[0][0] == 0:
        error(f'No egress run found with name "{name}"')
        return

    cur.execute('UPDATE egress_run SET status = "aborted" WHERE name = ?', (name,))
    conn.commit()


@use_db
def drop_egress(name: str, if_exists: bool = False) -> None:
    if not if_exists:
        cur.execute('SELECT count(*) FROM egresses WHERE name = ?', (name,))
        if cur.fetchall()[0][0] == 0:
            error(f'No egress found with name "{name}"')
            return

    cur.execute('DELETE FROM egresses WHERE name = ?', (name,))
    conn.commit()


class CreateCatalog(SQLHandler):
    """
    CREATE CATALOG [ if_not_exists ] name
        using_catalog_type
        [ with_table_format ]
    ;

    # Catalog name
    name = '<config-name>'

    # If not exists
    if_not_exists = IF NOT EXISTS

    # Table format
    with_table_format = WITH TABLE FORMAT ICEBERG [ _iceberg_version ]
    _iceberg_version = VERSION '<version>'

    # Catalog type
    using_catalog_type = USING { glue | HIVE | POLARIS | REST }

    # Glue parameters
    glue = GLUE _in_glue_region _with_glue_catalog_id
    _in_glue_region = IN REGION '<glue-region-name>'
    _with_glue_catalog_id = WITH CATALOG ID '<glue-catalog-id>'

    Description
    -----------
    Create a catalog configuration

    Arguments
    ---------
    * ``<catalog-config-name>``: The name of the catalog configuration.
    * ``<glue-region-name>``: Name of the region the AWS Glue catalog lives in.
    * ``<glue-catalog-id>``: ID of the AWS Glue catalog.

    Example
    -------
    The following command creates an Iceberg v2 catalog configuration using AWS Glue
    named 'example-iceberg'::

        CREATE CATALOG 'example-iceberg' USING GLUE
            IN REGION 'us-east-1' WITH CATALOG ID 'a1239823693'
            WITH TABLE FORMAT ICEBERG VERSION 2;

    """
    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        using_catalog_type = params['using_catalog_type']
        if isinstance(using_catalog_type, dict):
            catalog_type = list(using_catalog_type.keys())[0].lower()
            c_params = list(using_catalog_type.values())[0]
            catalog_params = dict(
                region=c_params['in_glue_region'],
                catalog_id=c_params['with_glue_catalog_id'],
            )
        else:
            catalog_type = using_catalog_type
            catalog_params = {}

        with_table_format = params['with_table_format']
        if isinstance(with_table_format, dict):
            table_format = 'iceberg'
            table_format_parameters = dict(version=with_table_format['iceberg_version'])
        else:
            table_format = 'iceberg'
            table_format_parameters = {}

        create_catalog(
            params['name'],
            catalog_type,
            catalog_params,
            table_format,
            table_format_parameters,
        )

        return None


CreateCatalog.register(overwrite=True)


class ShowCatalogs(SQLHandler):
    """
    SHOW CATALOGS [ extended ] [ <like> ];

    extended = EXTENDED

    Description
    -----------
    Show all catalog configurations.

    Example
    -------
    The following statement shows all catalog configuations with names beginning
    with "dev"::

        SHOW CATALOGS LIKE "dev%";

    """
    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        res = FusionSQLResult()

        if params['extended']:
            res.add_field('Name', result.STRING)
            res.add_field('Type', result.STRING)
            res.add_field('Params', result.JSON)
            res.add_field('TableFormat', result.STRING)
            res.add_field('TableFormatParams', result.JSON)
        else:
            res.add_field('Name', result.STRING)
            res.add_field('Type', result.STRING)
            res.add_field('TableFormat', result.STRING)

        res.set_rows(show_catalogs(**params))

        return res


ShowCatalogs.register(overwrite=True)


class DropCatalog(SQLHandler):
    """
    DROP CATALOG [ if_exists ] name;

    # If catalog exists
    if_exists = IF EXISTS

    # Catalog name
    name = '<config-name>'

    Description
    -----------
    Drop a catalog configuration.

    Arguments
    ---------
    * ``<config-name>``: Name of the catalog configuration to delete.

    Example
    -------
    The following statement deletes a catalog configuration with the name "dev-cat"::

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
    using_catalog = USING CATALOG '<config-name>'

    # Storage URL
    with_storage_base_url = WITH STORAGE BASE URL '<storage-url>'

    Description
    -----------
    Create a cluster identity for allowing the egress service to access
    external cloud resources.

    Example
    -------
    * ``<config-name>``: Name of the catalog configuration to use.
    * ``<storage-url>``: URL of the data storage location.

    Example
    -------
    The following statement creates a cluster identity for the catalog
    configuration name "dev-cat" and storage URL "s3:/bucket-name/iceberg"::

        CREATE CLUSTER IDENTITY
            USING CATALOG dev-cat
            WITH STORAGE BASE URL 's3://bucket-name/iceberg';

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
        using_catalog
        with_storage_base_url
        [ with_file_format ]
        [ with_update_mode ]
        [ ordered_by ]
        [ partitioned_by ]
    ;

    # If not exists
    if_not_exists = IF NOT EXISTS

    # Egress name
    name = '<egress-name>'

    # From table
    from_table = FROM <table>

    # Column names
    colnames = ( <column>,... )

    # With file format
    with_file_format = WITH FILE FORMAT _parquet

    # Parquet options
    _parquet = PARQUET [ ( __parquet_options,... ) ]
    __parquet_options = {
    &&    __row_group_size |
    &&    __page_size |
    &&    __page_row_limit |
    &&    __dict_page_size |
    &&    __compression_codec |
    &&    __compression_level |
    &&    __target_file_size
    & }
    __row_group_size = ROW_GROUP_SIZE = <integer> { KB | MB }
    __page_size = PAGE_SIZE = <integer> { KB | MB }
    __page_row_limit = PAGE_ROW_LIMIT = <integer>
    __dict_page_size = DICT_PAGE_SIZE = <integer> { KB | MB }
    __compression_codec = COMPRESSION_CODEC = { ZSTD | BROTLI | LZ4 | GZIP | SNAPPY | NONE }
    __compression_level = COMPRESSION_LEVEL = <integer>
    __target_file_size = TARGET_FILE_SIZE = <integer> { KB | MB }

    # Catolog config
    using_catalog = USING CATALOG '<catalog-config>'

    # Update mode
    with_update_mode = WITH UPDATE MODE { COPY_ON_WRITE | MERGE_ON_READ }

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

    # Storage URL
    with_storage_base_url = WITH STORAGE BASE URL _storage_url [ _storage_region ]
    _storage_url = '<url>'
    _storage_region = IN REGION '<region-name>'

    Description
    -----------
    Create an egress configuration.

    Arguments
    ---------
    * ``<egress-name>``: The name to give the egress configuration.
    * ``<column>``: Name of a column in the source table.
    * ``<catalog-config>``: Name of a catalog configuration.
    * ``<url>``: URL pointing to the data storage location.
    * ``<region-name>``: Name of region for storage location.

    Remarks
    -------
    * ``IF NOT EXISTS`` indicates that the egress configuration should only be
      created if there isn't one with the given name.
    * ``FROM <table>`` specifies the SingleStore table to egress. The same name will
      be used for the egressed table.
    * ``USING CATALOG`` specifies the name of a catalog configuration.
    * ``WITH STORAGE BASE URL`` indicates the location where output data files
      will be stored. ``IN REGION`` can be used to specify a region for cloud
      storage locations.
    * ``WITH FILE FORMAT`` specifies the format and parameters for the output
      data files.
    * ``WITH UPDATE MODE`` specifies the method in which deleted rows are handled.
    * ``ORDER BY`` indicates the sort order for the rows in the data files.
      ``ASC`` and ``DESC`` can be used to indicate ascending or descending order.
      ``NULLS FIRST`` and ``NULLS LAST`` can be used to indicate where nulls
      should occur in the sort order.
    * ``PARTITION BY`` indicates how the data files should be partitioned.

    Examples
    --------
    The following statement creates an egress configuration named "dev-egress" using
    catalog configuration named "dev-cat" and data storage URL "s3://bucket-name/iceberg".
    The source table to egress is named "customer_data"::

        CREATE EGRESS dev-egress FROM customer-data
            USING CATALOG dev-cat
            WITH STORAGE BASE URL "s3://bucket-name/iceberg";

    The following statement adds ordering and partitioning to the above configuration::

        CREATE EGRESS dev-egress FROM customer_data
            USING CATALOG dev-cat
            WITH STORAGE BASE URL "s3://bucket-name/iceberg"
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
            database, table = params['from_table']
        else:
            database = None
            table = params['from_table']

        # Columns
        columns = params['colnames']

        # Catalog
        catalog = params['using_catalog']

        # Storage
        sbu = params['with_storage_base_url']
        storage_base_url = sbu['storage_url']
        storage_region = sbu['storage_region']

        # File format
        file_format = list(params['with_file_format'].keys())[0]

        option_processor = getattr(self, f'_process_{file_format}_options', lambda x: x)

        # File format params
        file_format_params = {}
        for formats in params['with_file_format'].values():
            for opts in formats:
                for opt in opts.values():
                    file_format_params.update(option_processor(opt))

        # Update mode
        update_mode = params['with_update_mode'].lower()

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
            database,
            table,
            columns,
            catalog,
            storage_base_url,
            storage_region,
            file_format,
            file_format_params,
            update_mode,
            order_by,
            partition_by,
            if_not_exists=if_not_exists,
        )

        return None


CreateEgress.register(overwrite=True)


class ShowEgresses(SQLHandler):
    """
    SHOW EGRESSES [ extended ] [ <like> ];

    # Show more details
    extended = EXTENDED

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
        res.add_field('Database', result.STRING)
        res.add_field('Table', result.STRING)
        res.add_field('Columns', result.JSON)
        res.add_field('Catalog', result.STRING)
        res.add_field('StorageBaseURL', result.STRING)

        if params['extended']:
            res.add_field('StorageRegion', result.STRING)
            res.add_field('FileFormat', result.STRING)
            res.add_field('FileFormatParams', result.JSON)
            res.add_field('UpdateMode', result.STRING)
            res.add_field('OrderBy', result.JSON)
            res.add_field('PartitionBy', result.JSON)

        res.add_field('Status', result.STRING)

        res.set_rows(show_egresses(**params))

        return res


ShowEgresses.register(overwrite=True)


class ShowEgress(SQLHandler):
    """
    SHOW EGRESS name [ extended ];

    # Egress config name
    name = '<egress-name>'

    # Show more details
    extended = EXTENDED

    Description
    -----------
    Show egress configuration for given name.

    Arguments
    ---------
    * ``<egress-name>>``: Name of the egress configuration.

    Examples
    --------
    The following statement shows the egress configuration named "dev-egress"::

        SHOW EGRESS dev-egress;

    """
    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        res = FusionSQLResult()

        res.add_field('Name', result.STRING)
        res.add_field('Database', result.STRING)
        res.add_field('Table', result.STRING)
        res.add_field('Columns', result.JSON)
        res.add_field('Catalog', result.STRING)
        res.add_field('StorageBaseURL', result.STRING)

        if params['extended']:
            res.add_field('StorageRegion', result.STRING)
            res.add_field('FileFormat', result.STRING)
            res.add_field('FileFormatParams', result.JSON)
            res.add_field('UpdateMode', result.STRING)
            res.add_field('OrderBy', result.JSON)
            res.add_field('PartitionBy', result.JSON)

        res.add_field('Status', result.STRING)

        res.set_rows(show_egress(**params))

        return res


ShowEgress.register(overwrite=True)


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
    START EGRESS name [ if_not_running ];

    # Egress config name
    name = '<egress-name>'

    # Do not return error if already running
    if_not_running = IF NOT RUNNING

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
