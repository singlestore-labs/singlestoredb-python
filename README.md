# <img src="resources/singlestore-logo.png" height="60" valign="middle"/> SingleStore Python Interface

![PyPI](https://img.shields.io/pypi/v/singlestore)
![Conda](https://img.shields.io/conda/v/singlestore/singlestore)
![License](https://img.shields.io/github/license/singlestore-labs/singlestore-python)
![Python Version](https://img.shields.io/pypi/pyversions/singlestore)

This project contains a [DB-API 2.0](https://www.python.org/dev/peps/pep-0249/)
compatible Python interface to the SingleStore database and cluster management API.

## Install

This package can be install from PyPI using `pip`:
```
pip install singlestore
```

If you are using Anaconda, you can install with `conda`:
```
conda install -c singlestore singlestore
```

## Usage

Connections to the SingleStore database are made using the DB-API parameters
`host`, `port`, `user`, `password`, etc, but they may also be done using
URLs that specify these parameters as well (much like the
[SQLAlchemy](https://www.sqlalchemy.org) package).
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
conn = s2.connect('https://user:password@host:8080/db_name')
```


## Resources

* [SingleStore](https://singlestore.com)
* [Python](https://python.org)
