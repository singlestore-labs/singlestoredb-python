
.. image:: ../../resources/singlestore-logo.png
   :align: left
   :height: 50
   :width: 50

SingleStoreDB Python Client
===========================

.. raw:: html

   <br clear>

The SingleStoreDB Python client is a Python DB-API compliant database connector
for use with the SingleStore database. It also includes objects for interfacing
with SingleStoreDB Cloud's management API.

.. warning:: As of v0.5.0, the substition parameter has been changed from
   ``:1``, ``:2``, etc. for list parameters and ``:foo``, ``:bar``, etc.
   for dictionary parameters to ``%s`` and ``%(foo)s``, ``%(bar)s``, etc.
   respectively, to ease the transition from other MySQL Python packages.

.. toctree::
   :maxdepth: 3
   :caption: Contents:

   install
   whatsnew
   getting-started
   api
   license
