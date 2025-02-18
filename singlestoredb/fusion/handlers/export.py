#!/usr/bin/env python3
import json
from typing import Any
from typing import Dict
from typing import Optional

from .. import result
from ...management.export import ExportService
from ...management.export import ExportStatus
from ..handler import SQLHandler
from ..result import FusionSQLResult
from .utils import get_workspace_group


class CreateClusterIdentity(SQLHandler):
    """
    CREATE CLUSTER IDENTITY
        catalog
        storage
    ;

    # Catolog
    catalog = CATALOG { _catalog_config | _catalog_creds }
    _catalog_config = CONFIG '<catalog-config>'
    _catalog_creds = CREDENTIALS '<catalog-creds>'

    # Storage
    storage = LINK { _link_config | _link_creds }
    _link_config = S3 CONFIG '<link-config>'
    _link_creds = CREDENTIALS '<link-creds>'

    Description
    -----------
    Create a cluster identity for allowing the export service to access
    external cloud resources.

    Arguments
    ---------
    * ``<catalog-config>`` and ``<catalog-creds>``: Catalog configuration
      and credentials in JSON format.
    * ``<link-config>`` and ``<link-creds>``: Storage link configuration
      and credentials in JSON format.

    Remarks
    -------
    * ``CATALOG`` specifies the details of the catalog to connect to.
    * ``LINK`` specifies the details of the data storage to connect to.

    Example
    -------
    The following statement creates a cluster identity for the catalog
    and link::

        CREATE CLUSTER IDENTITY
            CATALOG CONFIG '{
                "catalog_type": "GLUE",
                "table_format": "ICEBERG",
                "catalog_id": "13983498723498",
                "catalog_region": "us-east-1"
            }'
            LINK S3 CONFIG '{
                "region": "us-east-1",
                "endpoint_url": "s3://bucket-name"

            }'
        ;

    """

    _enabled = False

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        # Catalog
        catalog_config = json.loads(params['catalog'].get('catalog_config', '{}') or '{}')
        catalog_creds = json.loads(params['catalog'].get('catalog_creds', '{}') or '{}')

        # Storage
        storage_config = json.loads(params['storage'].get('link_config', '{}') or '{}')
        storage_creds = json.loads(params['storage'].get('link_creds', '{}') or '{}')

        storage_config['provider'] = 'S3'

        wsg = get_workspace_group({})

        if wsg._manager is None:
            raise TypeError('no workspace manager is associated with workspace group')

        out = ExportService(
            wsg,
            'none',
            'none',
            dict(**catalog_config, **catalog_creds),
            dict(**storage_config, **storage_creds),
            columns=None,
        ).create_cluster_identity()

        res = FusionSQLResult()
        res.add_field('Identity', result.STRING)
        res.set_rows([(out['identity'],)])

        return res


CreateClusterIdentity.register(overwrite=True)


class CreateExport(SQLHandler):
    """
    START EXPORT
        from_table
        catalog
        storage
        [ partition_by ]
        [ order_by ]
        [ properties ]
    ;

    # From table
    from_table = FROM <table>

    # Transforms
    _col_transform = { VOID | IDENTITY | YEAR | MONTH | DAY | HOUR } ( _transform_col )
    _transform_col = <column>
    _arg_transform = { BUCKET | TRUNCATE } ( _transform_col <comma> _transform_arg )
    _transform_arg = <integer>
    transform = { _col_transform | _arg_transform }

    # Partitions
    partition_by = PARTITION BY partition_key,...
    partition_key = transform

    # Sort order
    order_by = ORDER BY sort_key,...
    sort_key = transform [ direction ] [ null_order ]
    direction = { ASC | DESC | ASCENDING | DESCENDING }
    null_order = { NULLS_FIRST | NULLS_LAST }

    # Properties
    properties = PROPERTIES '<json>'

    # Catolog
    catalog = CATALOG [ _catalog_config ] [ _catalog_creds ]
    _catalog_config = CONFIG '<catalog-config>'
    _catalog_creds = CREDENTIALS '<catalog-creds>'

    # Storage
    storage = LINK [ _link_config ] [ _link_creds ]
    _link_config = S3 CONFIG '<link-config>'
    _link_creds = CREDENTIALS '<link-creds>'

    Description
    -----------
    Create an export configuration.

    Arguments
    ---------
    * ``<catalog-config>`` and ``<catalog-creds>``: The catalog configuration.
    * ``<link-config>`` and ``<link-creds>``: The storage link configuration.

    Remarks
    -------
    * ``FROM <table>`` specifies the SingleStore table to export. The same name will
      be used for the exported table.
    * ``CATALOG`` specifies the details of the catalog to connect to.
    * ``LINK`` specifies the details of the data storage to connect to.

    Examples
    --------
    The following statement starts an export operation with the given
    catalog and link configurations. The source table to export is
    named "customer_data"::

        START EXPORT FROM my_db.customer_data
            CATALOG CONFIG '{
                "catalog_type": "GLUE",
                "table_format": "ICEBERG",
                "catalog_id": "13983498723498",
                "catalog_region": "us-east-1"
            }'
            LINK S3 CONFIG '{
                "region": "us-east-1",
                "endpoint_url": "s3://bucket-name"

            }'
        ;

    """  # noqa

    _enabled = False

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        # From table
        if isinstance(params['from_table'], str):
            from_database = None
            from_table = params['from_table']
        else:
            from_database, from_table = params['from_table']

        # Catalog
        catalog_config = json.loads(params['catalog'].get('catalog_config', '{}') or '{}')
        catalog_creds = json.loads(params['catalog'].get('catalog_creds', '{}') or '{}')

        # Storage
        storage_config = json.loads(params['storage'].get('link_config', '{}') or '{}')
        storage_creds = json.loads(params['storage'].get('link_creds', '{}') or '{}')

        storage_config['provider'] = 'S3'

        wsg = get_workspace_group({})

        if from_database is None:
            raise ValueError('database name must be specified for source table')

        if wsg._manager is None:
            raise TypeError('no workspace manager is associated with workspace group')

        partition_by = []
        if params['partition_by']:
            for key in params['partition_by']:
                transform = key['partition_key']['transform']['col_transform']
                part = {}
                part['transform'] = transform[0].lower()
                part['name'] = transform[-1]['transform_col']
                partition_by.append(part)

        order_by = []
        if params['order_by'] and params['order_by']['by']:
            for key in params['order_by']['by']:
                transform = key['transform']['col_transform']
                order = {}
                order['transform'] = transform[0].lower()
                order['name'] = transform[-1]['transform_col']
                order['direction'] = 'ascending'
                order['null_order'] = 'nulls_first'
                if key.get('direction'):
                    if 'desc' in key['direction'].lower():
                        order['direction'] = 'descending'
                if key.get('null_order'):
                    if 'last' in key['null_order'].lower():
                        order['null_order'] = 'nulls_last'
                order_by.append(order)

        out = ExportService(
            wsg,
            from_database,
            from_table,
            dict(**catalog_config, **catalog_creds),
            dict(**storage_config, **storage_creds),
            columns=None,
            partition_by=partition_by or None,
            order_by=order_by or None,
            properties=json.loads(params['properties']) if params['properties'] else None,
        ).start()

        res = FusionSQLResult()
        res.add_field('ExportID', result.STRING)
        res.set_rows([(out.export_id,)])

        return res


CreateExport.register(overwrite=True)


class ShowExport(SQLHandler):
    """
    SHOW EXPORT export_id;

    # ID of export
    export_id = '<export-id>'

    """

    _enabled = False

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        wsg = get_workspace_group({})
        out = ExportStatus(params['export_id'], wsg)

        status = out._info()

        res = FusionSQLResult()
        res.add_field('ExportID', result.STRING)
        res.add_field('Status', result.STRING)
        res.add_field('Message', result.STRING)
        res.set_rows([
            (
                params['export_id'],
                status.get('status', 'Unknown'),
                status.get('statusMsg', ''),
            ),
        ])

        return res


ShowExport.register(overwrite=True)
