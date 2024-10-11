#!/usr/bin/env python3
import json
from typing import Any
from typing import Dict
from typing import Optional

from ...management import manage_workspaces
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
        wsm = manage_workspaces()
        wsg = get_workspace_group({})

        # Catalog
        catalog_config = json.loads(params['catalog']['catalog_config'] or '{}')
        catalog_creds = json.loads(params['catalog']['catalog_creds'] or '{}')

        # Storage
        storage_config = json.loads(params['storage']['link_config'] or '{}')
        storage_creds = json.loads(params['storage']['link_creds'] or '{}')

        EgressService(
            wsg,
            'dummy',
            'dummy',
            Catalog.from_config_and_creds(catalog_config, catalog_creds, wsm),
            Link.from_config_and_creds('S3', storage_config, storage_creds, wsm),
            columns=None,
        ).create_cluster_identity()

        return None


CreateClusterIdentity.register(overwrite=True)


class CreateEgress(SQLHandler):
    """
    CREATE EGRESS [ if_not_exists ] name
        from_table
        catalog
        storage
        [ properties ]
        [ description ]
    ;

    # If not exists
    if_not_exists = IF NOT EXISTS

    # Egress name
    name = '<egress-name>'

    # From table
    from_table = FROM <table>

    # Properties
    properties = PROPERTIES '<table-properties>'

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
    Create an egress configuration.

    Arguments
    ---------
    * ``<egress-name>``: The name to give the egress configuration.
    * ``<catalog-name>``: Name of a catalog profile.
    * ``<link-name>``: Name of the link for accessing data storage.
    * ``<table-properties>``: Table properties as a JSON object.
    * ``<description>``: Description of egress.

    Remarks
    -------
    * ``IF NOT EXISTS`` indicates that the egress configuration should only be
      created if there isn't one with the given name.
    * ``FROM <table>`` specifies the SingleStore table to egress. The same name will
      be used for the egressed table.
    * ``CATALOG`` specifies the name of a catalog profile.
    * ``LINK`` indicates the name of a link for accessing storage.

    Examples
    --------
    The following statement creates an egress configuration named "dev-egress" using
    catalog and link configurations.  The source table to egress is
    named "customer_data"::

        CREATE EGRESS dev-egress FROM customer_data
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
        print(params)
        return None

        # Name
        # if_not_exists = params['if_not_exists']
        # name = params['name']

        # From table
        if len(params['from_table']) > 1:
            from_database, from_table = params['from_table']
        else:
            from_database = None
            from_table = params['from_table']

        # Catalog
        catalog_config = json.loads(params['catalog']['catalog_config'] or '{}')
        catalog_creds = json.loads(params['catalog']['catalog_creds'] or '{}')

        # Storage
        storage_config = json.loads(params['storage']['link_config'] or '{}')
        storage_creds = json.loads(params['storage']['link_creds'] or '{}')

        # Properties
        # properties = json.loads(params['properties'] or '{}')

        wsm = manage_workspaces()
        wsg = get_workspace_group({})

        EgressService(
            wsg,
            from_database,
            from_table,
            Catalog.from_config_and_creds(catalog_config, catalog_creds, wsm),
            Link.from_config_and_creds('S3', storage_config, storage_creds, wsm),
            columns=None,
        ).create_cluster_identity()

        return None


CreateEgress.register(overwrite=True)
