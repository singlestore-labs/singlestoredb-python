# SingleStore Python Interface

This project contains a Python interface to the SingleStore database
and cluster management API. The database interface is more of a meta-API
that wraps various MySQL-compatible Python interfaces and normalizes
the differences and also adds support for the SingleStore HTTP API.
This simply means that you can use your choice of various interfaces all 
behind the same Python programming API.

## Install

This package can be install from PyPI using `pip`:
```
pip install singlestore
```

The default installation includes the `pymysql` and `requests` packages
which can access the SingleStore database using pure Python code. To 
install additional interfaces such as the MySQLdb connector or the 
official MySQL Python connector (`mysql.connector`) which may offer
better performance, either install the connectors separately or include
them as an optional install:
```
# Include the MySQLdb connector
pip install singlestore[MySQLdb]

# Include the MySQL Python connector
pip install singlestore[mysqlconnector]

# Include the pyodbc connector
pip install singlestore[pyodbc]

# Include the cymysql connector
pip install singlestore[cymysql]
```

## Usage

Connections to the SingleStore database are made using URLs that specify
the connection driver package, server hostname, server port, and user
credentials. All connections support the
[Python DB-API 2.0](https://www.python.org/dev/peps/pep-0249/).
By default, the `pymysql` connector is used since it works on all platforms.
However, you may want to select other connectors for better performance.
```
import singlestore as s2

# Connect using the pymysql connector
conn = s2.connect('user:password@host:3306/db_name')

# Explicitly specifying the connector works as well
# conn = s2.connect('pymysql://user:password@host:3306/db_name')

# Create a cursor
cur = conn.cursor()

# Execute SQL
cur.execute('select * from foo;')

# Fetch the results
print(cur.description)
for item in cur:
    print(item)

# Close the connection
conn.close()
```

The above example used the `pymysql` connector for doing the actual
communication with the server using the MySQL wire protocol. You can
use other MySQL-compatible packages as well.
```
# Use the MySQLdb connector
conn = s2.connect('mysqldb://user:password@host:3306/db_name')

# Use the mysql.connector connector
conn = s2.connect('mysqlconnector://user:password@host:3306/db_name')

# Use the HTTP API connector
conn = s2.connect('http://user:password@host:3306/db_name')
```


## Resources

* [SingleStore](https://singlestore.com)
* [Python](https://python.org)
