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
   WorkspaceManager.regions_v2
   WorkspaceManager.shared_tier_regions
   WorkspaceManager.billing
   WorkspaceManager.create_workspace_group
   WorkspaceManager.create_workspace
   WorkspaceManager.create_starter_workspace
   WorkspaceManager.get_workspace_group
   WorkspaceManager.get_workspace
   WorkspaceManager.get_starter_workspace
   WorkspaceManager.invitations
   WorkspaceManager.get_invitation
   WorkspaceManager.create_invitation
   WorkspaceManager.users
   WorkspaceManager.current_user
   WorkspaceManager.get_user
   WorkspaceManager.add_user
   WorkspaceManager.teams
   WorkspaceManager.get_team
   WorkspaceManager.create_team
   WorkspaceManager.get_roles
   WorkspaceManager.get_role
   WorkspaceManager.create_role
   WorkspaceManager.secrets
   WorkspaceManager.get_secret_by_id
   WorkspaceManager.create_secret
   WorkspaceManager.get_audit_logs
   WorkspaceManager.create_private_connection
   WorkspaceManager.get_private_connection


WorkspaceGroup
..............

WorkspaceGroup objects are retrieved from :meth:`WorkspaceManager.get_workspace_group`
or by retrieving an element from :attr:`WorkspaceManager.workspace_groups`.

.. autosummary::
   :toctree: generated/

   WorkspaceGroup
   WorkspaceGroup.workspaces
   WorkspaceGroup.stage
   WorkspaceGroup.storage
   WorkspaceGroup.create_workspace
   WorkspaceGroup.refresh
   WorkspaceGroup.update
   WorkspaceGroup.terminate
   WorkspaceGroup.private_connections
   WorkspaceGroup.get_access_controls
   WorkspaceGroup.update_access_controls
   WorkspaceGroup.get_metrics


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
   Workspace.suspend
   Workspace.resume
   Workspace.private_connections
   Workspace.get_kai_private_connection_info
   Workspace.get_outbound_allow_list


Region
......

Region objects are accessed from the :attr:`WorkspaceManager.regions` attribute
or through the :class:`RegionManager`.

.. currentmodule:: singlestoredb.management.region

.. autosummary::
   :toctree: generated/

   Region


RegionManager
^^^^^^^^^^^^^

RegionManager provides methods for listing available regions.

.. autosummary::
   :toctree: generated/

   RegionManager
   RegionManager.list_regions
   RegionManager.list_shared_tier_regions


Organization
............

Organization objects are retrieved from :attr:`WorkspaceManager.organization`.
They provide access to organization-level resources and operations.

.. currentmodule:: singlestoredb.management.organization

.. autosummary::
   :toctree: generated/

   Organization
   Organization.get_secret
   Organization.jobs
   Organization.inference_apis
   Organization.get_access_controls
   Organization.update_access_controls


Secret
......

Secret objects are retrieved from :meth:`Organization.get_secret`.
They represent organization-wide secrets that can be used in various operations.

.. autosummary::
   :toctree: generated/

   Secret
   Secret.update
   Secret.delete
   Secret.get_access_controls
   Secret.update_access_controls


Cluster Management
..................

The :func:`manage_cluster` function provides access to cluster management functionality.

.. currentmodule:: singlestoredb

.. autosummary::
   :toctree: generated/

   manage_cluster


ClusterManager
^^^^^^^^^^^^^^

ClusterManager objects are returned by the :func:`manage_cluster` function.
They allow you to create and manage clusters.

.. currentmodule:: singlestoredb.management.cluster

.. autosummary::
   :toctree: generated/

   ClusterManager
   ClusterManager.clusters
   ClusterManager.regions
   ClusterManager.create_cluster
   ClusterManager.get_cluster


Cluster
^^^^^^^

Cluster objects represent SingleStore clusters. They can be created using
:meth:`ClusterManager.create_cluster` or retrieved from :attr:`ClusterManager.clusters`.

.. autosummary::
   :toctree: generated/

   Cluster
   Cluster.connect
   Cluster.refresh
   Cluster.update
   Cluster.suspend
   Cluster.resume
   Cluster.terminate


User Management
...............

User objects represent users in your organization.

.. currentmodule:: singlestoredb.management.users

.. autosummary::
   :toctree: generated/

   User
   User.get_identity_roles
   User.remove


Team Management
...............

Team objects allow you to organize users into groups.

.. currentmodule:: singlestoredb.management.teams

.. autosummary::
   :toctree: generated/

   Team
   Team.refresh
   Team.update
   Team.delete
   Team.get_access_controls
   Team.update_access_controls
   Team.get_identity_roles


Role Management
...............

Role objects define permissions within your organization.

.. currentmodule:: singlestoredb.management.roles

.. autosummary::
   :toctree: generated/

   Role
   Role.update
   Role.delete


Invitation Management
.....................

Invitation objects represent invitations to join your organization.

.. currentmodule:: singlestoredb.management.invitations

.. autosummary::
   :toctree: generated/

   Invitation
   Invitation.revoke


Audit Logging
.............

Audit logs track changes and actions within your organization.

.. currentmodule:: singlestoredb.management.audit_logs

.. autosummary::
   :toctree: generated/

   AuditLog
   AuditLogResult


Private Connections
...................

Private connections allow you to establish private network connectivity
to your workspaces.

.. currentmodule:: singlestoredb.management.private_connections

.. autosummary::
   :toctree: generated/

   PrivateConnection
   PrivateConnection.refresh
   PrivateConnection.update
   PrivateConnection.delete
   OutboundAllowListEntry
   KaiPrivateConnectionInfo


Billing
.......

Billing objects provide access to usage and billing information.

.. currentmodule:: singlestoredb.management.workspace

.. autosummary::
   :toctree: generated/

   Billing
   Billing.usage

.. currentmodule:: singlestoredb.management.billing_usage

.. autosummary::
   :toctree: generated/

   UsageItem
   BillingUsageItem


Starter Workspaces
..................

Starter workspaces provide a free tier option for development and testing.

.. currentmodule:: singlestoredb.management.workspace

.. autosummary::
   :toctree: generated/

   StarterWorkspace
   StarterWorkspace.connect
   StarterWorkspace.terminate
   StarterWorkspace.refresh
   StarterWorkspace.create_user
   StarterWorkspace.get_user
   StarterWorkspaceUser
   StarterWorkspaceUser.update
   StarterWorkspaceUser.delete


Storage and Disaster Recovery
.............................

Storage objects provide access to workspace group storage settings
and disaster recovery functionality.

.. currentmodule:: singlestoredb.management.storage

.. autosummary::
   :toctree: generated/

   Storage
   Storage.dr
   Storage.update_retention_period


DisasterRecovery
^^^^^^^^^^^^^^^^

DisasterRecovery objects manage disaster recovery configuration and operations.

.. autosummary::
   :toctree: generated/

   DisasterRecovery
   DisasterRecovery.get_status
   DisasterRecovery.get_regions
   DisasterRecovery.setup
   DisasterRecovery.failover
   DisasterRecovery.failback
   DisasterRecovery.start_pre_provision
   DisasterRecovery.stop_pre_provision
   DRStatus
   DRRegion


Export Service
..............

Export services allow you to replicate data from SingleStore to external systems.

.. currentmodule:: singlestoredb.management.export

.. autosummary::
   :toctree: generated/

   ExportService
   ExportService.create_cluster_identity
   ExportService.start
   ExportService.suspend
   ExportService.resume
   ExportService.drop
   ExportService.status
   ExportStatus


Inference API
.............

Inference API management allows you to deploy and manage ML models.

.. currentmodule:: singlestoredb.management.inference_api

.. autosummary::
   :toctree: generated/

   InferenceAPIManager
   InferenceAPIManager.get
   InferenceAPIManager.start
   InferenceAPIManager.stop
   InferenceAPIManager.show
   InferenceAPIManager.drop
   InferenceAPIInfo
   InferenceAPIInfo.start
   InferenceAPIInfo.stop
   InferenceAPIInfo.drop
   ModelOperationResult


Projects
........

Project objects represent projects within your organization.

.. currentmodule:: singlestoredb.management.projects

.. autosummary::
   :toctree: generated/

   Project


Jobs Management
...............

The jobs management functionality allows you to schedule and manage notebook executions.

JobsManager
^^^^^^^^^^^

JobsManager objects are accessed through :attr:`Organization.jobs` and provide
methods for creating, monitoring, and managing scheduled notebook jobs.

.. currentmodule:: singlestoredb.management.job

.. autosummary::
   :toctree: generated/

   JobsManager
   JobsManager.schedule
   JobsManager.run
   JobsManager.wait
   JobsManager.get
   JobsManager.get_executions
   JobsManager.get_parameters
   JobsManager.delete
   JobsManager.modes
   JobsManager.runtimes


Job
^^^

Job objects represent scheduled notebook executions and are returned by
:meth:`JobsManager.schedule`, :meth:`JobsManager.run`, and :meth:`JobsManager.get`.

.. autosummary::
   :toctree: generated/

   Job
   Job.wait
   Job.get_executions
   Job.get_parameters
   Job.delete


Supporting Classes
^^^^^^^^^^^^^^^^^^

The following classes are used as parameters and return values in the jobs API.

.. autosummary::
   :toctree: generated/

   Mode
   TargetType
   Status
   Parameter
   Runtime
   JobMetadata
   ExecutionMetadata
   Execution
   ExecutionsData
   ExecutionConfig
   Schedule
   TargetConfig


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
