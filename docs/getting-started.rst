.. currentmodule:: singlestore

Getting Started
===============

Connections to SingleStore can be made using the parameters described in
the Python DB-API.

.. ipython:: python
   :suppress:

   import singlestore as s2
   conn = s2.connect()

.. ipython:: python
   :verbatim:

   import singlestore as s2
   conn = s2.connect(host='...', port='...', user='...',
                     password='...', database='...')

In addition, you can user a URL like in the SQLAlchemy package.

.. ipython:: python
   :verbatim:

   conn = s2.connect('user:password@host:port/database')

URLs work equally well to connect to the HTTP API.

.. ipython:: python
   :verbatim:

   conn = s2.connect('https://user:password@host:port/database')


Executing Queries
-----------------

Once you have a connection established, you can query the database.
As defined in the DB-API, a cursor is used to execute queries and fetch
the results.

.. ipython:: python

   with conn.cursor() as cur:
        cur.execute('show variables like "auto%"')
        for row in cur.fetchall():
            print(*row)
