#!/usr/bin/env python
"""Iceberg Materialized View handler for SingleStoreDB Fusion."""
from __future__ import annotations

import json
import uuid
from typing import Any
from typing import Dict
from typing import Optional

from singlestoredb.connection import connect
from singlestoredb.fusion import result
from singlestoredb.fusion.handler import SQLHandler


class CreateIcebergMaterializedView(SQLHandler):
    """
    CREATE ICEBERG MATERIALIZED VIEW
        [ if_not_exists ]
        view_name
        ON iceberg_table
        [ catalog ]
        [ storage ]
    ;

    # If not exists
    if_not_exists = IF NOT EXISTS

    # View name
    view_name = <table>

    # Iceberg table
    iceberg_table = <table>

    # Catalog
    catalog = CATALOG [ _catalog_config ] [ _catalog_creds ]
    _catalog_config = CONFIG '<catalog-config>'
    _catalog_creds = CREDENTIALS '<catalog-creds>'

    # Storage
    storage = LINK [ _link_config ] [ _link_creds ]
    _link_config = S3 CONFIG '<link-config>'
    _link_creds = CREDENTIALS '<link-creds>'

    Description
    -----------
    Create an Iceberg materialized view that syncs data from an Iceberg table
    to a SingleStore table with automatic updates.

    Arguments
    ---------
    * ``<catalog-config>`` and ``<catalog-creds>``: The catalog configuration.
    * ``<link-config>`` and ``<link-creds>``: The storage link configuration.

    Remarks
    -------
    * ``CATALOG`` specifies the details of the catalog to connect to.
    * ``LINK`` specifies the details of the data storage to connect to.
    * The materialized view will keep the SingleStore table in sync with the
      Iceberg table through an underlying pipeline.

    Examples
    --------
    The following statement creates an Iceberg materialized view::

        CREATE ICEBERG MATERIALIZED VIEW my_db.all_sales_orders
        ON my_catalog.sales.orders
        CATALOG CONFIG '{
            "catalog_type": "GLUE",
            "table_format": "ICEBERG",
            "catalog_id": "123456789012",
            "catalog_region": "us-east-1"
        }'
        LINK S3 CONFIG '{
            "region": "us-east-1",
            "endpoint_url": "s3://my-bucket"
        }'
        ;

    """

    def run(self, params: Dict[str, Any]) -> Optional[result.FusionSQLResult]:
        # Parse view name
        if isinstance(params['view_name'], str):
            view_database = None
            view_table = params['view_name']
        else:
            view_database, view_table = params['view_name']

        # Parse Iceberg table reference
        iceberg_parts = params['iceberg_table']
        if isinstance(iceberg_parts, str):
            # Simple table name
            catalog_name = None
            iceberg_database = None
            iceberg_table = iceberg_parts
        elif len(iceberg_parts) == 2:
            # database.table
            catalog_name = None
            iceberg_database, iceberg_table = iceberg_parts
        elif len(iceberg_parts) == 3:
            # catalog.database.table
            catalog_name, iceberg_database, iceberg_table = iceberg_parts
        else:
            raise ValueError(
                'Iceberg table reference must be in format: '
                '[catalog.]database.table',
            )

        # Iceberg expects lowercase
        if iceberg_database:
            iceberg_database = iceberg_database.lower()
        if catalog_name:
            catalog_name = catalog_name.lower()

        # Parse catalog configuration
        catalog_config = json.loads(
            params['catalog'].get('catalog_config', '{}') or '{}',
        )
        catalog_creds = json.loads(
            params['catalog'].get('catalog_creds', '{}') or '{}',
        )

        # Parse storage configuration
        storage_config = json.loads(
            (params.get('storage') or {}).get('link_config', '{}') or '{}',
        )
        storage_creds = json.loads(
            (params.get('storage') or {}).get('link_creds', '{}') or '{}',
        )

        storage_config['provider'] = 'S3'

        # Validate required fields
        if iceberg_database is None:
            raise ValueError(
                'Database name must be specified for Iceberg table',
            )

        if view_database is None:
            with connect() as conn:
                with conn.cursor() as cur:
                    cur.execute('SELECT DATABASE()')
                    res = cur.fetchone()
                    if not res:
                        raise ValueError(
                            'No database selected. Please specify database '
                            'name for materialized view',
                        )
                    if isinstance(res, (tuple, list)):
                        view_database = res[0]
                    elif isinstance(res, dict):
                        view_database = list(res.values())[0]
                    else:
                        raise ValueError(
                            'Unexpected result type from SELECT DATABASE()',
                        )

        # Merge configurations
        config = {}
        config.update(catalog_config)
        config.update(storage_config)
        config['table_id'] = f'{iceberg_database}.{iceberg_table}'
        config_json = json.dumps(config)

        creds = {}
        creds.update(catalog_creds)
        creds.update(storage_creds)
        creds_json = json.dumps(creds)

        # Create a unique pipeline name
        pipeline_name = f'iceberg_mv_{view_database}_{view_table}_{uuid.uuid4().hex[:8]}'

        print('ICEBERG TABLE', iceberg_database, iceberg_table)
        print('DB TABLE', view_database, view_table)
        print('CONFIG', config)
        print('CREDS', creds)

        # Create and start the pipeline
        with connect() as conn:
            with conn.cursor() as cur:
                # Create the pipeline
                cur.execute(rf'''
                    CREATE PIPELINE `{pipeline_name}` AS
                        LOAD DATA S3 ''
                        CONFIG '{config_json}'
                        CREDENTIALS '{creds_json}'
                        REPLACE INTO TABLE
                            `{view_database}`.`{view_table}`
                        FORMAT ICEBERG
                ''')

                # Start the pipeline
                cur.execute(rf'START PIPELINE `{pipeline_name}`')

        # Return result
        res = result.FusionSQLResult()
        res.add_field('MaterializedView', result.STRING)
        res.add_field('Pipeline', result.STRING)
        res.add_field('Status', result.STRING)
        res.set_rows([
            (f'{view_database}.{view_table}', pipeline_name, 'Created'),
        ])

        return res


CreateIcebergMaterializedView.register(overwrite=True)
