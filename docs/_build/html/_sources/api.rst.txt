.. module:: singlestore
.. _api:

API Reference
=============

.. _api.functions:


Connections
-----------

The `connect` function is the primary entry point for the SingleStore
package. It connects to a SingleStore database using either DB-API
compliant parameters, or a connection string in the form of a URL.

.. autosummary::
   :toctree: generated/

   connect


Connection
..........

Connection objects are created by the :func:`connect` function. They are
used to create :class:`Cursor` objects for querying the database.

.. currentmodule:: singlestore.connection

.. autosummary::
   :toctree: generated/

   Connection.autocommit
   Connection.close
   Connection.commit
   Connection.rollback
   Connection.cursor
   Connection.is_connected
   Connection.set_global_var
   Connection.get_global_var
   Connection.set_session_var
   Connection.get_session_var
   Connection.enable_http_api
   Connection.disable_http_api


Cursor
......

Cursors are used to query the database and download results.  They are
created using the :meth:`Connection.cursor` method.

.. currentmodule:: singlestore.connection

.. autosummary::
   :toctree: generated/

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


Cluster Management
------------------

The cluster management objects allow you to create, destroy, and interact with
clusters in the SingleStore managed service.

.. currentmodule:: singlestore

.. autosummary::
   :toctree: generated/

   manage_cluster

ClusterManager
..............

ClusterManager objects are returned by the :func:`manage_cluster` function.
They allow you to retrieve information about clusters in your account, or
create new ones.

.. currentmodule:: singlestore.manager

.. autosummary::
   :toctree: generated/

   ClusterManager.clusters
   ClusterManager.regions
   ClusterManager.create_cluster
   ClusterManager.get_cluster

Cluster
.......

Cluster objects are retrieved from :meth:`ClusterManager.get_cluster` or
by retrieving an element from :attr:`CluterManager.clusters`.

.. autosummary::
   :toctree: generated/

   Cluster
   Cluster.refresh
   Cluster.update
   Cluster.suspend
   Cluster.resume
   Cluster.terminate
   Cluster.connect

Region
......

Region objects are accessed from the :attr:`ClusterManager.regions` attribute.

.. autosummary::
   :toctree: generated/

   Region


Configuration
-------------

The following functions are used to get and set package configuration settings.
Execute the :func:`describe_option` function with no parameters to see the
documentation for all options.

.. currentmodule:: singlestore

.. autosummary::
   :toctree: generated/

   get_option
   set_option
   describe_option
