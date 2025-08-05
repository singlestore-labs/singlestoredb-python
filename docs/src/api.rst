.. module:: singlestoredb
.. _api:

API Reference
=============

.. _api.functions:


Connections
-----------

The :func:`connect` function is the primary entry point for the SingleStore
package. It connects to a SingleStore database using either
`DB-API <https://peps.python.org/pep-0249/>`_ compliant parameters,
or a connection string in the form of a URL.

The :func:`vector_db` function gives you an interface for working with
an API better for working with vector data.

The :func:`create_engine` function is used with the SQLAlchemy package
to create an SQLAlchemy engine for SingleStoreDB connections. This is
primarily for use in environments where the connection parameters are
stored in environment variables so that you can create SingleStoreDB
connections without specifying any parameters in the code itself.

.. autosummary::
   :toctree: generated/

   connect
   vector_db
   create_engine


Connection
..........

Connection objects are created by the :func:`singlestoredb.connect` function. They are
used to create :class:`Cursor` objects for querying the database.

.. currentmodule:: singlestoredb.connection

.. autosummary::
   :toctree: generated/

   Connection
   Connection.autocommit
   Connection.close
   Connection.commit
   Connection.rollback
   Connection.cursor
   Connection.is_connected
   Connection.enable_data_api
   Connection.disable_data_api
   Connection.vector_db


The :attr:`Connection.show` attribute of the connection objects allow you to access various
information about the server. The available operations are shown below.

.. currentmodule:: singlestoredb.connection

.. autosummary::
   :toctree: generated/

   ShowAccessor.aggregates
   ShowAccessor.columns
   ShowAccessor.create_aggregate
   ShowAccessor.create_function
   ShowAccessor.create_pipeline
   ShowAccessor.create_table
   ShowAccessor.create_view
   ShowAccessor.databases
   ShowAccessor.database_status
   ShowAccessor.errors
   ShowAccessor.functions
   ShowAccessor.global_status
   ShowAccessor.indexes
   ShowAccessor.partitions
   ShowAccessor.pipelines
   ShowAccessor.plan
   ShowAccessor.plancache
   ShowAccessor.procedures
   ShowAccessor.processlist
   ShowAccessor.reproduction
   ShowAccessor.schemas
   ShowAccessor.session_status
   ShowAccessor.status
   ShowAccessor.table_status
   ShowAccessor.tables
   ShowAccessor.warnings


ShowResult
^^^^^^^^^^

The results of the above methods and attributes are in the form of a
:class:`ShowResult` object. This object is primarily used to display
information to the screen or web browser, but columns from the output
can also be accessed using dictionary-like key access syntax or
attributes.

.. currentmodule:: singlestoredb.connection

.. autosummary::
   :toctree: generated/

   ShowResult


Cursor
......

Cursors are used to query the database and download results.  They are
created using the :meth:`Connection.cursor` method.

.. currentmodule:: singlestoredb.connection

.. autosummary::
   :toctree: generated/

   Cursor
   Cursor.callproc
   Cursor.close
   Cursor.execute
   Cursor.executemany
   Cursor.fetchone
   Cursor.fetchmany
   Cursor.fetchall
   Cursor.nextset
   Cursor.setinputsizes
   Cursor.setoutputsize
   Cursor.scroll
   Cursor.next
   Cursor.is_connected


Uploading from streaming sources
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

While the standard way of loading a local file using ``LOAD DATA LOCAL INFILE``
is well-known, it is possible to load data from programmatic sources
as well. This means that you can generate data with a Python function
and load that data into the database without writing it to a file
first. Below are two methods of doing this. Both of them use the
``local_infile=true`` parameter on the connection and the ``infile_stream=``
option on the ``cursor.execute`` method.

Uploading data using a generator function
:::::::::::::::::::::::::::::::::::::::::

.. sourcecode:: python

   conn = s2.connect('s2-host.com/my_db?local_infile=true')
   cur = conn.cursor()

   def upload_csv():
       yield '1,2,3\n'
       yield '4,5,6\n'
       yield '7,8,9\n'
       # Note that the data returned does not have to
       # correspond to a single row. Any length of
       # data can be returned.
       yield '10,11,12\n13,14,15\n'

   cur.execute('CREATE TABLE generator (a INT, b INT, c INT)')

   cur.execute(
       '''
       LOAD DATA LOCAL INFILE ':stream:' INTO TABLE generator
           FIELDS TERMINATED BY ',' ENCLOSED BY '"'
       ''',
       infile_stream=upload_csv(),
   )

Uploading data using a queue and threads
::::::::::::::::::::::::::::::::::::::::

.. sourcecode:: python

   from queue import Queue
   from threading import Thread

   conn = s2.connect('s2-host.com/my_db?local_infile=true')
   cur = conn.cursor()

   data_feeder = Queue()

   cur.execute('CREATE TABLE queue (a INT, b INT, c INT)')

   t = Thread(
       target=cur.execute,
       args=(
           '''
           LOAD DATA LOCAL INFILE ':stream:' INTO TABLE queue
               FIELDS TERMINATED BY ',' ENCLOSED BY '"'
           ''',
       ),
       kwargs=dict(infile_stream=data_feeder),
   )
   t.start()

   data_feeder.put('1,2,3\n')
   data_feeder.put('4,5,6\n')
   data_feeder.put('7,8,9\n')
   # Note that the data sent does not have to
   # correspond to a single row. Any length of
   # data can be sent.
   data_feeder.put('10,11,12\n13,14,15\n')
   # Send an empty string to end the data loader.
   data_feeder.put('')

   t.join()


Utilities
---------

.. currentmodule:: singlestoredb.auth

.. autosummary::
   :toctree: generated/

   get_jwt


Management API
--------------

The management objects allow you to create, destroy, and interact with
workspaces in the SingleStoreDB Cloud.

The :func:`manage_workspaces` function will return a :class:`WorkspaceManager`
object that can be used to interact with the Management API.

.. currentmodule:: singlestoredb

.. autosummary::
   :toctree: generated/

   manage_workspaces
   manage_teams
   manage_users
   manage_audit_logs
   manage_private_connections
   manage_storage_dr
   manage_metrics


WorkspaceManager
................

WorkspaceManager objects are returned by the :func:`manage_workspaces` function.
They allow you to retrieve information about workspaces in your account, or
create new ones.

.. currentmodule:: singlestoredb.management.workspace

.. autosummary::
   :toctree: generated/

   WorkspaceManager
   WorkspaceManager.organization
   WorkspaceManager.workspace_groups
   WorkspaceManager.starter_workspaces
   WorkspaceManager.regions
   WorkspaceManager.shared_tier_regions
   WorkspaceManager.create_workspace_group
   WorkspaceManager.create_workspace
   WorkspaceManager.create_starter_workspace
   WorkspaceManager.get_workspace_group
   WorkspaceManager.get_workspace
   WorkspaceManager.get_starter_workspace


WorkspaceGroup
..............

WorkspaceGroup objects are retrieved from :meth:`WorkspaceManager.get_workspace_group`
or by retrieving an element from :attr:`WorkspaceManager.workspace_groups`.

.. autosummary::
   :toctree: generated/

   WorkspaceGroup
   WorkspaceGroup.workspaces
   WorkspaceGroup.stage
   WorkspaceGroup.create_workspace
   WorkspaceGroup.refresh
   WorkspaceGroup.update
   WorkspaceGroup.terminate


Workspace
.........

Workspaces are created within WorkspaceGroups. They can be created using either
:meth:`WorkspaceGroup.create_workspace` or retrieved from
:attr:`WorkspaceManager.workspaces`.

.. autosummary::
   :toctree: generated/

   Workspace
   Workspace.connect
   Workspace.refresh
   Workspace.update
   Workspace.terminate


Region
......

Region objects are accessed from the :attr:`WorkspaceManager.regions` attribute.

.. currentmodule:: singlestoredb.management.region

.. autosummary::
   :toctree: generated/

   Region


Stage Files
...........

To interact with files in your Stage, use the
:attr:`WorkspaceGroup.stage` attribute.
It will return a :class:`Stage` object which defines the following
methods and attributes.

.. currentmodule:: singlestoredb.management.workspace

.. autosummary::
   :toctree: generated/

   Stage
   Stage.open
   Stage.download_file
   Stage.download_folder
   Stage.upload_file
   Stage.upload_folder
   Stage.info
   Stage.listdir
   Stage.exists
   Stage.is_dir
   Stage.is_file
   Stage.mkdir
   Stage.rename
   Stage.remove
   Stage.removedirs
   Stage.rmdir


Personal, Shared, and Model Files
.................................

To manage personal files, shared files, and model files in an organization,
you use the :func:`singlestoredb.manage_files` function. This will return a
:class:`FilesManager` instance to access each location. You then use the
returned :class:`FileSpace` to manage the files in each location. These
classes are described below.

.. currentmodule:: singlestoredb

.. autosummary::
   :toctree: generated/

   manage_files

The :class:`FilesManager` gives you access to each of the personal,
shared, and model file locations.

.. currentmodule:: singlestoredb.management.files

.. autosummary::
   :toctree: generated/

   FilesManager
   FilesManager.personal_space
   FilesManager.shared_space
   FilesManager.models_space

The :class:`FileSpace` contains the methods for interacting with the
personal, shared, or model files.

.. currentmodule:: singlestoredb.management.files

.. autosummary::
   :toctree: generated/

   FileSpace
   FileSpace.open
   FileSpace.download_file
   FileSpace.download_folder
   FileSpace.upload_file
   FileSpace.upload_folder
   FileSpace.info
   FileSpace.listdir
   FileSpace.exists
   FileSpace.is_dir
   FileSpace.is_file
   FileSpace.mkdir
   FileSpace.rename
   FileSpace.remove
   FileSpace.removedirs
   FileSpace.rmdir


FilesObject
...........

:class:`FilesObject`s are returned by the :meth:`StageObject.upload_file`
:meth:`FilesObject.upload_folder`, :meth:`FilesObject.mkdir`,
:meth:`FilesObject.rename`, and :meth:`FilesObject.info` methods.

.. currentmodule:: singlestoredb.management.workspace

.. autosummary::
   :toctree: generated/

   FilesObject
   FilesObject.open
   FilesObject.download
   FilesObject.exists
   FilesObject.is_dir
   FilesObject.is_file
   FilesObject.abspath
   FilesObject.basename
   FilesObject.dirname
   FilesObject.getmtime
   FilesObject.getctime
   FilesObject.rename
   FilesObject.remove
   FilesObject.removedirs
   FilesObject.rmdir


TeamsManager
............

TeamsManager objects are returned by the :func:`manage_teams` function.
They allow you to create, retrieve, and manage teams in your organization.

.. currentmodule:: singlestoredb.management.teams

.. autosummary::
   :toctree: generated/

   TeamsManager
   TeamsManager.create_team
   TeamsManager.get_team
   TeamsManager.list_teams
   TeamsManager.teams
   TeamsManager.delete_team
   TeamsManager.update_team
   TeamsManager.get_team_identity_roles


Team
....

Team objects are retrieved from :meth:`TeamsManager.get_team` or by
retrieving an element from :attr:`TeamsManager.teams`.

.. autosummary::
   :toctree: generated/

   Team
   Team.update
   Team.delete
   Team.refresh
   Team.identity_roles


UsersManager
............

UsersManager objects are returned by the :func:`manage_users` function.
They allow you to retrieve and manage users in your organization.

.. currentmodule:: singlestoredb.management.users

.. autosummary::
   :toctree: generated/

   UsersManager
   UsersManager.get_user
   UsersManager.get_user_identity_roles
   UsersManager.create_user_invitation
   UsersManager.get_user_invitation
   UsersManager.list_user_invitations
   UsersManager.delete_user_invitation
   UsersManager.user_invitations


User
....

User objects are retrieved from :meth:`UsersManager.get_user`.

.. autosummary::
   :toctree: generated/

   User
   User.identity_roles


UserInvitation
..............

UserInvitation objects are returned by the various UsersManager invitation methods.

.. autosummary::
   :toctree: generated/

   UserInvitation


AuditLogsManager
................

AuditLogsManager objects are returned by the :func:`manage_audit_logs` function.
They allow you to retrieve and analyze audit logs for your organization.

.. currentmodule:: singlestoredb.management.audit_logs

.. autosummary::
   :toctree: generated/

   AuditLogsManager
   AuditLogsManager.list_audit_logs
   AuditLogsManager.audit_logs
   AuditLogsManager.get_audit_logs_for_user
   AuditLogsManager.get_audit_logs_for_resource
   AuditLogsManager.get_failed_actions
   AuditLogsManager.get_actions_by_type


AuditLog
........

AuditLog objects are returned by the various AuditLogsManager methods.

.. autosummary::
   :toctree: generated/

   AuditLog


PrivateConnectionsManager
.........................

PrivateConnectionsManager objects are returned by the :func:`manage_private_connections` function.
They allow you to create and manage private connections in your organization.

.. currentmodule:: singlestoredb.management.private_connections

.. autosummary::
   :toctree: generated/

   PrivateConnectionsManager
   PrivateConnectionsManager.create_private_connection
   PrivateConnectionsManager.get_private_connection
   PrivateConnectionsManager.private_connections
   PrivateConnectionsManager.delete_private_connection
   PrivateConnectionsManager.update_private_connection


PrivateConnection
.................

PrivateConnection objects are retrieved from :meth:`PrivateConnectionsManager.get_private_connection`
or by retrieving an element from :attr:`PrivateConnectionsManager.private_connections`.

.. autosummary::
   :toctree: generated/

   PrivateConnection


PrivateConnectionKaiInfo
........................

PrivateConnectionKaiInfo objects contain KAI-specific information for private connections.

.. autosummary::
   :toctree: generated/

   PrivateConnectionKaiInfo


PrivateConnectionOutboundAllowList
..................................

PrivateConnectionOutboundAllowList objects contain outbound allow list information for private connections.

.. autosummary::
   :toctree: generated/

   PrivateConnectionOutboundAllowList


IdentityRole
............

IdentityRole objects are used by both teams and users management for role information.

.. currentmodule:: singlestoredb.management.teams

.. autosummary::
   :toctree: generated/

   IdentityRole


StorageDRManager
................

StorageDRManager objects are returned by the :func:`manage_storage_dr` function.
They allow you to manage storage disaster recovery for your organization.

.. currentmodule:: singlestoredb.management.storage_dr

.. autosummary::
   :toctree: generated/

   StorageDRManager
   StorageDRManager.get_status
   StorageDRManager.get_available_regions
   StorageDRManager.setup_storage_dr
   StorageDRManager.start_failover
   StorageDRManager.start_failback
   StorageDRManager.start_pre_provision
   StorageDRManager.stop_pre_provision


Storage DR
----------

Storage Disaster Recovery objects provide information about replicated databases
and disaster recovery regions.

.. autosummary::
   :toctree: generated/

   ReplicatedDatabase
   StorageDRStatus
   StorageDRRegion
   StorageDRCompute


MetricsManager
..............

MetricsManager objects are returned by the :func:`manage_metrics` function.
They allow you to retrieve metrics for your organization.

.. currentmodule:: singlestoredb.management.metrics

.. autosummary::
   :toctree: generated/

   MetricsManager
   MetricsManager.get_workspace_group_metrics


Metrics
-------

Metrics objects provide workspace group metrics and data points.

.. autosummary::
   :toctree: generated/

   WorkspaceGroupMetrics
   MetricDataPoint


Billing Usage
-------------

Billing Usage objects provide usage and billing information for workspaces.

.. currentmodule:: singlestoredb.management.billing_usage

.. autosummary::
   :toctree: generated/

   UsageItem
   BillingUsageItem


Notebook Tools
--------------

The SDK includes a `notebook` sub-module for tools that are for use in
the `SingleStore Managed Service Portal Notebooks <https://portal.singlestore.com>`_
environment. The following objects in `sinlgestoredb.notebook` are
singletons that automatically track the organization, workspace group, workspace,
stage, and secrets that are selected in the portal.

These objects act just like the corresponding objects discussed in previous
of this documentation (including attributes and methods), but they proxy all
calls to the currently selected portal services.

.. currentmodule:: singlestoredb.notebook

.. autosummary::
   :toctree: generated/

   organization
   secrets
   workspace_group
   stage
   workspace


Server Tools
------------

It is possible to start a SingleStoreDB server from the Python SDK in a
couple of ways: Docker or Cloud Free Tier. Both of these methods are
described below.

Docker
......

If you have Docker installed on your machine, you can use the Docker interface
included with the Python SDK to start a SingleStoreDB server in Docker. This
allows you to open interactive shells, SQL Studio, and also use the `%sql`
magic commands from Jupysql with a SingleStoreDB server running in a container.

An example of starting SingleStoreDB in Docker is shown below.

.. sourcecode:: python


   from singlestoredb.server import docker

   s2db = docker.start()

   with s2db.connect() as conn:
       with conn.cursor() as cur:
           cur.execute('SHOW DATABASES')
           for line in cur:
               print(line)

   s2db.stop()

It is possible to use the server instance as a context manager as well.
This will automatically shut down the container after exiting the ``with``
block.

.. sourcecode:: python

   from singlestoredb.server import docker

   with docker.start() as s2db:
       with s2db.connect() as conn:
           with conn.cursor() as cur:
               cur.execute('SHOW DATABASES')
               for line in cur:
                   print(line)

If you do not explicitly shut down the container, it will get shut
down when the Python process exits.

.. currentmodule:: singlestoredb.server.docker

.. autosummary::
   :toctree: generated/

   start
   SingleStoreDB.logs
   SingleStoreDB.connect
   SingleStoreDB.connect_kai
   SingleStoreDB.connection_url
   SingleStoreDB.http_connection_url
   SingleStoreDB.kai_url
   SingleStoreDB.studio_url
   SingleStoreDB.open_studio
   SingleStoreDB.open_shell
   SingleStoreDB.open_mongosh
   SingleStoreDB.stop


Cloud Free Tier
...............

The Cloud Free Tier interface works much in the same way as the Docker
interface, although it doesn't support as many features. To start a
server in the Cloud Free Tier, you use the following:

.. sourcecode:: python


   from singlestoredb.server import free_tier

   s2db = free_tier.start()

   with s2db.connect() as conn:
       with conn.cursor() as cur:
           cur.execute('SHOW DATABASES')
           for line in cur:
               print(line)

   s2db.stop()

Just as with the Docker interface, you can also use a Python
context manager to automatically shut down your connection.

.. currentmodule:: singlestoredb.server.free_tier

.. autosummary::
   :toctree: generated/

   start
   SingleStoreDB.connect
   SingleStoreDB.connection_url
   SingleStoreDB.http_connection_url
   SingleStoreDB.kai_url
   SingleStoreDB.open_shell
   SingleStoreDB.open_mongosh
   SingleStoreDB.stop


Configuration
-------------

The following functions are used to get and set package configuration settings.
Execute the :func:`describe_option` function with no parameters to
see the documentation for all options.

.. currentmodule:: singlestoredb

.. autosummary::
   :toctree: generated/

   get_option
   set_option
   describe_option

In addition to the function above, you can access options through the
``singlestoredb.options`` object. This gives you attribute-like access to the option
values.

.. ipython:: python

   import singlestoredb as s2

   s2.describe_option('local_infile')

   s2.options.local_infile

   s2.options.local_infile = True

   s2.describe_option('local_infile')

.. ipython:: python
   :suppress:

   s2.options.local_infile = False
