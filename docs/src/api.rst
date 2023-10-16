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
   WorkspaceGroup.stages
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
   Workspace.terminate


Region
......

Region objects are accessed from the :attr:`WorkspaceManager.regions` attribute.

.. currentmodule:: singlestoredb.management.region

.. autosummary::
   :toctree: generated/

   Region


Stages
......

.. currentmodule:: singlestoredb.management.workspace

.. autosummary::
   :toctree: generated/

   Stages
   Stages.open
   Stages.upload_file
   Stages.upload_folder
   Stages.mkdir
   Stages.rename
   Stages.info
   Stages.exists
   Stages.is_dir
   Stages.is_file
   Stages.listdir
   Stages.download
   Stages.remove
   Stages.removedirs
   Stages.rmdir


StagesObject
............

.. currentmodule:: singlestoredb.management.workspace

.. autosummary::
   :toctree: generated/

   StagesObject
   StagesObject.download
   StagesObject.remove
   StagesObject.rmdir
   StagesObject.removedirs
   StagesObject.rename
   StagesObject.exists
   StagesObject.is_dir
   StagesObject.is_file
   StagesObject.abspath
   StagesObject.basename
   StagesObject.dirname
   StagesObject.getmtime
   StagesObject.getctime


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
