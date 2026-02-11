# <img src="https://raw.githubusercontent.com/singlestore-labs/singlestoredb-python/main/resources/singlestore-logo.png" height="60" valign="middle"/> SingleStoreDB Python SDK

The SingleStoreDB Python SDK provides a [DB-API 2.0](https://www.python.org/dev/peps/pep-0249/)
compatible interface to [SingleStore](https://www.singlestore.com/), a high-performance
distributed SQL database designed for data-intensive applications including real-time
analytics and vector search.

**Key Features:**

- **High Performance**: Includes a C extension that delivers up to 10x faster data
  reading compared to pure Python MySQL connectors
- **Multiple Protocols**: Connect via MySQL protocol (port 3306) or HTTP Data API
  (port 9000) using the same interface
- **Flexible Result Formats**: Return query results as tuples, dictionaries, named
  tuples, NumPy arrays, Pandas DataFrames, Polars DataFrames, or PyArrow Tables
- **Workspace Management**: Full API for managing SingleStore Cloud workspaces,
  clusters, regions, and files programmatically
- **Vector Store**: Pinecone-compatible vector database API for similarity search
  applications with built-in connection pooling
- **User-Defined Functions**: Deploy Python functions as SingleStore UDFs with
  automatic type mapping
- **SQLAlchemy Support**: Integrate with SQLAlchemy through the optional
  `sqlalchemy-singlestoredb` adapter
- **Fusion SQL**: Extend SQL with custom client-side command handlers

## Install

This package can be installed from PyPI using `pip`:
```
pip install singlestoredb
```

### Optional Dependencies

The SDK has several optional dependencies for additional functionality:

```bash
# Vector store support (Pinecone-compatible API)
pip install 'singlestoredb[vectorstore]'

# SQLAlchemy integration
pip install 'singlestoredb[sqlalchemy]'

# dbt adapter
pip install 'singlestoredb[dbt]'

# Kerberos/GSSAPI authentication
pip install 'singlestoredb[kerberos]'

# RSA key authentication
pip install 'singlestoredb[rsa]'

# Ed25519 key authentication
pip install 'singlestoredb[ed22519]'

# Multiple extras can be combined
pip install 'singlestoredb[vectorstore,sqlalchemy]'
```

| Extra | Description |
|-------|-------------|
| `vectorstore` | Vector database functionality via [singlestore-vectorstore](https://github.com/singlestore-labs/singlestore-vectorstore) |
| `sqlalchemy` | [SQLAlchemy](https://www.sqlalchemy.org/) dialect via [sqlalchemy-singlestoredb](https://github.com/singlestore-labs/sqlalchemy-singlestoredb) |
| `ibis` / `dataframe` | [Ibis](https://ibis-project.org/) dataframe interface (SingleStore is natively supported in Ibis) |
| `dbt` | [dbt](https://www.getdbt.com/) adapter via [dbt-singlestore](https://github.com/singlestore-labs/dbt-singlestore) |
| `kerberos` / `gssapi` | Kerberos/GSSAPI authentication support |
| `rsa` | RSA key exchange for encrypted connections |
| `ed22519` | Ed25519 key authentication |

## Documentation

https://singlestore-labs.github.io/singlestoredb-python

## Usage

Connections to the SingleStore database are made using the DB-API parameters
`host`, `port`, `user`, `password`, etc, but they may also be done using
URLs that specify these parameters as well (much like the
[SQLAlchemy](https://www.sqlalchemy.org) package).
```python
import singlestoredb as s2

# Connect using the default connector
conn = s2.connect('user:password@host:3306/db_name')

# Create a cursor
cur = conn.cursor()

# Execute SQL
cur.execute('select * from foo')

# Fetch the results
print(cur.description)
for item in cur:
    print(item)

# Close the connection
conn.close()
```

Connecting to the HTTP API is done as follows:
```python
# Use the HTTP API connector
conn = s2.connect('https://user:password@host:8080/db_name')
```

## Configuration

Connection parameters can be set through environment variables, programmatically,
or via URL. Environment variables are useful for keeping credentials out of code.

### Environment Variables

Key environment variables include:
- `SINGLESTOREDB_URL`: Full connection URL
- `SINGLESTOREDB_HOST`: Database hostname
- `SINGLESTOREDB_PORT`: Database port
- `SINGLESTOREDB_USER`: Username
- `SINGLESTOREDB_PASSWORD`: Password
- `SINGLESTOREDB_DATABASE`: Default database name
- `SINGLESTOREDB_PURE_PYTHON`: Set to `1` to disable C acceleration

### Programmatic Configuration

Options can be set and retrieved using `get_option`, `set_option`, and `option_context`:
```python
import singlestoredb as s2

# Get current option value
current_host = s2.get_option('host')

# Set an option
s2.set_option('results_type', 'pandas')

# Temporarily override options using a context manager
with s2.option_context('results_type', 'dicts'):
    conn = s2.connect()
    # results will be returned as dicts within this block
```

## Result Formats

Query results can be returned in various formats using the `results_type` parameter
or option. Supported formats include:
- `tuples` (default): Standard Python tuples
- `namedtuples`: Named tuples with column names as attributes
- `dicts`: Dictionaries with column names as keys
- `numpy`: NumPy arrays
- `pandas`: Pandas DataFrames
- `polars`: Polars DataFrames
- `arrow`: PyArrow Tables

```python
import singlestoredb as s2

conn = s2.connect('user:password@host/db_name')
cur = conn.cursor()

# Return results as dictionaries
cur.execute('SELECT * FROM customers', results_type='dicts')
for row in cur:
    print(row['customer_name'])

# Return results as a Pandas DataFrame
cur.execute('SELECT * FROM customers', results_type='pandas')
df = cur.fetchone()
```

## Management API

The SDK provides a workspace management API for managing SingleStore deployments
programmatically. This includes creating and managing workspaces, clusters,
regions, and files.

```python
import singlestoredb as s2

# Get a workspace manager (uses SINGLESTOREDB_MANAGEMENT_TOKEN env var by default)
manager = s2.manage_workspaces()

# List all workspaces
for ws in manager.workspaces:
    print(ws.name, ws.state)

# Create a new workspace
ws = manager.workspaces.create(
    name='my-workspace',
    workspace_group_id='<group-id>',
)
```

See the [API documentation](https://singlestore-labs.github.io/singlestoredb-python)
for full details on workspace, cluster, region, and file management.

## Vector Store

The SDK includes vector database functionality for similarity search
applications. This requires the `singlestore-vectorstore` package.

```python
import singlestoredb as s2

# Create a vector database connection with connection pooling
vdb = s2.vector_db(
    'user:password@host/db_name',
    pool_size=5,       # Number of connections in pool
    max_overflow=10,   # Maximum extra connections
    timeout=30,        # Connection acquisition timeout
)

# Use vector operations
index = vdb.get_or_create_index('my_index', dimension=768)
```

The `vector_db` function returns a VectorDB instance that provides
Pinecone-compatible operations for vector similarity search.

## Fusion SQL

Fusion SQL extends the SQL commands handled by the client with custom
handlers. These commands are processed locally rather than sent to the
database server. Built-in handlers provide SQL-like commands for managing
workspaces, running notebook jobs, and more.

```python
import os
os.environ['SINGLESTOREDB_FUSION_ENABLED'] = '1'

import singlestoredb as s2
conn = s2.connect()

# Show available cloud regions
conn.execute('SHOW REGIONS')

# List workspace groups
conn.execute('SHOW WORKSPACE GROUPS')

# List workspaces in a specific group
conn.execute("SHOW WORKSPACES IN GROUP 'my-group' EXTENDED")

# Create a new workspace group
conn.execute("""
    CREATE WORKSPACE GROUP 'analytics-team'
    IN REGION 'US West 2 (Oregon)'
    WITH PASSWORD 'my-password'
    WITH FIREWALL RANGES '10.0.0.0/8'
""")
```

See [singlestoredb/fusion/README.md](singlestoredb/fusion/README.md)
for details on writing custom Fusion SQL handlers.

## Advanced Options

### SSL/TLS Configuration

SSL connections can be configured using connection parameters or environment
variables:
```python
conn = s2.connect(
    'user:password@host/db_name',
    ssl_ca='/path/to/ca.pem',
    ssl_cert='/path/to/client-cert.pem',
    ssl_key='/path/to/client-key.pem',
)
```

### Other Connection Options

```python
conn = s2.connect(
    'user:password@host/db_name',

    # Performance options
    pure_python=False,          # Set True to disable C acceleration
    buffered=True,              # Buffer entire result set in memory

    # Data handling
    autocommit=True,            # Enable autocommit mode
    local_infile=True,          # Allow LOAD DATA LOCAL INFILE
    nan_as_null=True,           # Treat NaN as NULL in parameters
    inf_as_null=True,           # Treat Inf as NULL in parameters

    # Extended types
    enable_extended_data_types=True,  # Enable BSON and vector types
    vector_data_format='binary',      # Vector format: 'json' or 'binary'

    # Connection behavior
    connect_timeout=10,         # Connection timeout in seconds
    multi_statements=True,      # Allow multiple statements per query
)
```

## Performance

While this package is based on [PyMySQL](https://github.com/PyMySQL/PyMySQL)
which is a pure Python-based MySQL connector, it adds various performance
enhancements that make it faster than most other connectors. The performance
improvements come from changes to the data conversion functions, cursor implementations,
and a C extension that is highly optimized to improve row data reading.

The package can be used both in a pure Python mode and as well as a C accelerated
mode. Generally speaking, the C accelerated version of the client can read
data 10X faster than PyMySQL, 2X faster than MySQLdb, and 1.5X faster than
mysql.connector. All of this is done without having to install any 3rd party
MySQL libraries!

Benchmarking was done with a table of 3,533,286 rows each containing a datetime,
a float, and eight character columns. The data is the same data set used in
[this article](https://www.singlestore.com/blog/how-to-get-started-with-singlestore/).
The client and server were running on the same machine and queries were made
using `fetchone`, `fetchall`, `fetchmany(1000)`, and an iterator over the cursor
object (e.g., `iter(cur)`). The results are shown below.

### Buffered

|                         | PyMySQL | MySQLdb | mysql.connector | SingleStore (pure Python) | SingleStore |
|-------------------------|---------|---------|-----------------|---------------------------|-------------|
| fetchall                |   37.0s |    8.7s |            5.6s |                     29.0s |        3.7s |
| fetchmany(1000)         |   37.4s |    9.2s |            6.2s |                     29.6s |        3.6s |
| fetchone                |   38.2s |   10.1s |            10.2s |                     30.9s |        4.8s |
| iter(cur)               |   38.3s |    9.1s |            10.2s |                     30.4s |        4.4s |

### Unbuffered

|                         | PyMySQL | MySQLdb | mysql.connector | SingleStore (pure Python) | SingleStore |
|-------------------------|---------|---------|-----------------|---------------------------|-------------|
| fetchall                |   39.0s |    6.5s |            5.5s |                     30.3s |        5.5s |
| fetchmany(1000)         |   39.4s |    7.0s |            6.0s |                     30.4s |        4.1s |
| fetchone                |   34.5s |    8.9s |           10.1s |                     30.8s |        6.6s |
| iter(cur)               |   39.0s |    9.0s |           10.2s |                     31.4s |        6.0s |


## License

This library is licensed under the [Apache 2.0 License](https://raw.githubusercontent.com/singlestore-labs/singlestoredb-python/main/LICENSE?token=GHSAT0AAAAAABMGV6QPNR6N23BVICDYK5LAYTVK5EA).

## Resources

* [SingleStore](https://singlestore.com)
* [Python](https://python.org)
