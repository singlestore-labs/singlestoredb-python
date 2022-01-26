# <img src="resources/singlestore-logo.png" height="60" valign="middle"/> SingleStore Python Interface

This project contains a [DB-API 2.0](https://www.python.org/dev/peps/pep-0249/).
compatible Python interface to the SingleStore database and cluster management API.

## Install

This package can be install from PyPI using `pip`:
```
pip install singlestore
```

## Usage

Connections to the SingleStore database are made using URLs that specify
the connection driver package, server hostname, server port, and user
credentials. All connections support the
[Python DB-API 2.0](https://www.python.org/dev/peps/pep-0249/).
```
import singlestore as s2

# Connect using the default connector
conn = s2.connect('user:password@host:3306/db_name')

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

Connecting to the HTTP API is done as follows:
```
# Use the HTTP API connector
conn = s2.connect('http://user:password@host:8080/db_name')
```


## Resources

* [SingleStore](https://singlestore.com)
* [Python](https://python.org)
