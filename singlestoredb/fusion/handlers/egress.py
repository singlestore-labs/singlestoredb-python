#!/usr/bin/env python3
import json
from typing import Any
from typing import Dict
from typing import Optional

from .. import result
from ...management.egress import Catalog
from ...management.egress import EgressService
from ...management.egress import Link
from ..handler import SQLHandler
from ..result import FusionSQLResult
from .utils import get_workspace_group


class CreateClusterIdentity(SQLHandler):
    """
    CREATE CLUSTER IDENTITY
        catalog
        storage
        [ description ]
    ;

    # Catolog
    catalog = CATALOG { _catalog_config | _catalog_creds | _catalog_name }
    _catalog_config = CONFIG '<catalog-config>'
    _catalog_creds = CREDENTIALS '<catalog-creds>'
    _catalog_name = <catalog-name>

    # Storage
    storage = LINK { _link_config | _link_creds | _link_name }
    _link_config = S3 CONFIG '<link-config>'
    _link_creds = CREDENTIALS '<link-creds>'
    _link_name = <link-name>

    # Description
    description = DESCRIPTION '<description>'

    Description
    -----------
    Create a cluster identity for allowing the egress service to access
    external cloud resources.

    Arguments
    ---------
    * ``<catalog-name>``: Name of the catalog profile to use.
    * ``<link-name>``: Name of the link profile to use.

    Remarks
    -------
    * ``CATALOG`` specifies the name of a catalog profile.
    * ``LINK`` indicates the name of a link for accessing storage.

    Example
    -------
    The following statement creates a cluster identity for the catalog
    and link::

        CREATE CLUSTER IDENTITY
            CATALOG ...
            LINK ...
        ;

    """
    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        # Catalog
        catalog_config = json.loads(params['catalog'].get('catalog_config', '{}') or '{}')
        catalog_creds = json.loads(params['catalog'].get('catalog_creds', '{}') or '{}')

        # Storage
        storage_config = json.loads(params['storage'].get('link_config', '{}') or '{}')
        storage_creds = json.loads(params['storage'].get('link_creds', '{}') or '{}')

        wsg = get_workspace_group({})

        if wsg._manager is None:
            raise TypeError('no workspace manager is associated with workspace group')

        out = EgressService(
            wsg,
            'none',
            'none',
            Catalog.from_config_and_creds(catalog_config, catalog_creds, wsg._manager),
            Link.from_config_and_creds('S3', storage_config, storage_creds, wsg._manager),
            columns=None,
        ).create_cluster_identity()

        res = FusionSQLResult()
        res.add_field('RoleARN', result.STRING)
        res.set_rows([(out['roleARN'],)])

        return res


CreateClusterIdentity.register(overwrite=True)


class CreateEgress(SQLHandler):
    """
    START EGRESS
        from_table
        catalog
        storage
        [ properties ]
        [ description ]
    ;

    # From table
    from_table = FROM <table>

    # Properties
    properties = PROPERTIES '<table-properties>'

    # Catolog
    catalog = CATALOG [ _catalog_config ] [ _catalog_creds ]
    _catalog_config = CONFIG '<catalog-config>'
    _catalog_creds = CREDENTIALS '<catalog-creds>'

    # Storage
    storage = LINK [ _link_config ] [ _link_creds ]
    _link_config = S3 CONFIG '<link-config>'
    _link_creds = CREDENTIALS '<link-creds>'

    # Description
    description = DESCRIPTION '<description>'

    Description
    -----------
    Create an egress configuration.

    Arguments
    ---------
    * ``<catalog-config>`` and ``<catalog-creds>``: The catalog configuration.
    * ``<link-config>`` and ``<link-creds>``: The storage link configuration.
    * ``<table-properties>``: Table properties as a JSON object.
    * ``<description>``: Description of egress.

    Remarks
    -------
    * ``FROM <table>`` specifies the SingleStore table to egress. The same name will
      be used for the egressed table.
    * ``CATALOG`` specifies the name of a catalog profile.
    * ``LINK`` indicates the name of a link for accessing storage.

    Examples
    --------
    The following statement starts an egress operation with the given
    catalog and link configurations.  The source table to egress is
    named "customer_data"::

        START EGRESS FROM customer_data
            CATALOG CONFIG '{

            }'
            LINK S3 CONFIG '{

            }'
            PROPERTIES '{
                "write.update.mode": "copy-on-write",
                "write.format.default": "parquet",
                "write.parquet.row-group-size-bytes": 50000000,
                "write.target-file-size-bytes": 100000000
            }';

    """  # noqa

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

        # Properties
        # properties = json.loads(params['properties'] or '{}')

        wsg = get_workspace_group({})

        if from_database is None:
            raise ValueError('database name must be specified for source table')

        if wsg._manager is None:
            raise TypeError('no workspace manager is associated with workspace group')

        out = EgressService(
            wsg,
            from_database,
            from_table,
            Catalog.from_config_and_creds(catalog_config, catalog_creds, wsg._manager),
            Link.from_config_and_creds('S3', storage_config, storage_creds, wsg._manager),
            columns=None,
        ).start()

        res = FusionSQLResult()
        res.add_field('EgressID', result.STRING)
        res.set_rows([(out['egressID'],)])

        return res


CreateEgress.register(overwrite=True)
