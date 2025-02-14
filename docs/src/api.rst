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

The :func:`create_engine` function is used with the SQLAlchemy package
to create an SQLAlchemy engine for SingleStoreDB connections. This is
primarily for use in environments where the connection parameters are
stored in environment variables so that you can create SingleStoreDB
connections without specifying any parameters in the code itself.

.. autosummary::
   :toctree: generated/

   connect
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
   WorkspaceManager.regions
   WorkspaceManager.create_workspace_group
   WorkspaceManager.create_workspace
   WorkspaceManager.get_workspace_group
   WorkspaceManager.get_workspace


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


Stage
.....

To interact with Stage, use the :attr:`WorkspaceManager.stage` attribute.
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


StageObject
...........

:class:`StageObject`s are returned by the :meth:`StageObject.upload_file`
:meth:`StageObject.upload_folder`, :meth:`StageObject.mkdir`,
:meth:`StageObject.rename`, and :meth:`StageObject.info` methods.

.. currentmodule:: singlestoredb.management.workspace

.. autosummary::
   :toctree: generated/

   StageObject
   StageObject.open
   StageObject.download
   StageObject.exists
   StageObject.is_dir
   StageObject.is_file
   StageObject.abspath
   StageObject.basename
   StageObject.dirname
   StageObject.getmtime
   StageObject.getctime
   StageObject.rename
   StageObject.remove
   StageObject.removedirs
   StageObject.rmdir


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
