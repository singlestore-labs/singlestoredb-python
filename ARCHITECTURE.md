# SingleStoreDB Python SDK Architecture

This document describes the architecture of the SingleStoreDB Python SDK, explaining how
the components fit together, their responsibilities, and their interactions.

## Table of Contents

1. [Overview](#overview)
2. [Database Connectivity](#database-connectivity)
3. [Configuration System](#configuration-system)
4. [Management API](#management-api)
5. [Fusion SQL](#fusion-sql)
6. [External Functions (UDFs)](#external-functions-udfs)
7. [AI Integration](#ai-integration)
8. [Vector Store](#vector-store)
9. [Supporting Modules](#supporting-modules)
10. [Testing Infrastructure](#testing-infrastructure)
11. [Additional Integrations](#additional-integrations)
12. [Appendices](#appendices)

---

## Overview

### Purpose

The SingleStoreDB Python SDK provides:
- **DB-API 2.0 compliant interface** to SingleStore databases
- **Cloud management API** for workspace and cluster lifecycle management
- **Fusion SQL** for client-side SQL command extension
- **External Functions** (UDFs) for deploying Python functions to SingleStore
- **AI integrations** for chat and embeddings

### Design Principles

1. **Protocol Abstraction**: Single `connect()` function handles both MySQL wire protocol
   and HTTP API connections transparently
2. **C Acceleration**: Optional C extension provides 10x faster row parsing with pure
   Python fallback
3. **Extensive Result Formats**: Support for tuples, dicts, namedtuples, numpy, pandas,
   polars, and arrow formats
4. **Environment-based Configuration**: All parameters configurable via environment
   variables

### Package Structure

```
singlestoredb/
├── __init__.py              # Public API exports
├── connection.py            # DB-API 2.0 connection/cursor abstraction
├── config.py                # Global configuration system
├── exceptions.py            # Exception hierarchy
├── types.py                 # DB-API type objects
├── converters.py            # Data type conversion
├── auth.py                  # JWT authentication
├── vectorstore.py           # Vector database integration
├── pytest.py                # Testing infrastructure
│
├── mysql/                   # MySQL protocol connector
│   ├── connection.py        # MySQL connection implementation
│   ├── cursors.py           # 20+ cursor types with mixins
│   ├── protocol.py          # Wire protocol implementation
│   ├── _auth.py             # Authentication methods
│   ├── converters.py        # MySQL-specific converters
│   ├── charset.py           # Character set handling
│   └── constants/           # Protocol constants
│
├── http/                    # HTTP API connector
│   └── connection.py        # REST-based connection
│
├── management/              # Cloud management API
│   ├── manager.py           # Base REST client
│   ├── workspace.py         # Workspace/WorkspaceGroup/Stage
│   ├── organization.py      # Organization management
│   ├── region.py            # Region definitions
│   ├── job.py               # Job management
│   ├── files.py             # File operations
│   ├── billing_usage.py     # Billing and usage tracking
│   └── export.py            # Data export operations
│
├── fusion/                  # Client-side SQL extensions
│   ├── handler.py           # SQLHandler base class
│   ├── registry.py          # Handler registration
│   ├── result.py            # FusionSQLResult
│   └── handlers/            # Built-in handlers
│       ├── workspace.py     # Workspace commands
│       ├── stage.py         # Stage commands
│       ├── job.py           # Job commands
│       ├── files.py         # File commands
│       ├── models.py        # Model commands
│       └── export.py        # Export commands
│
├── functions/               # External functions (UDFs)
│   ├── decorator.py         # @udf decorator
│   ├── signature.py         # Type signature handling
│   ├── dtypes.py            # Data type mapping
│   └── ext/                 # Execution modes
│       ├── asgi.py          # HTTP/ASGI server
│       ├── mmap.py          # Memory-mapped execution
│       ├── json.py          # JSON serialization
│       ├── rowdat_1.py      # ROWDAT_1 format
│       └── arrow.py         # Apache Arrow format
│
├── ai/                      # AI/ML integration
│   ├── chat.py              # Chat completion factory
│   └── embeddings.py        # Embeddings factory
│
├── alchemy/                 # SQLAlchemy integration
├── notebook/                # Jupyter notebook support
├── magics/                  # IPython magic commands
├── server/                  # Server management tools
└── tests/                   # Test suite
```

---

## Database Connectivity

The primary use case of the SDK is connecting to SingleStore databases. The connection
layer provides a unified interface with protocol-specific implementations.

### Connection Architecture

The entry point is `singlestoredb.connect()` in `singlestoredb/connection.py:1312`:

```python
import singlestoredb as s2

# MySQL protocol (default)
conn = s2.connect('user:password@host:3306/database')

# HTTP API
conn = s2.connect('http://user:password@host:9000/database')
```

**Protocol Selection Flow:**

```
┌─────────────────────────────────────────────────────────────────────┐
│                        connect(url, **params)                       │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
                    ┌─────────────────────────────┐
                    │  _parse_url() + build_params()  │
                    │  Extract driver from scheme │
                    └─────────────────────────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              ▼                    ▼                    ▼
    ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
    │ driver='mysql'  │  │ driver='http'   │  │ driver='https'  │
    │ or None/empty   │  │                 │  │                 │
    └─────────────────┘  └─────────────────┘  └─────────────────┘
              │                    │                    │
              ▼                    └──────────┬─────────┘
    ┌───────────────────┐                     ▼
    │ mysql.Connection  │          ┌───────────────────┐
    │ (PyMySQL-based)   │          │ http.Connection   │
    └───────────────────┘          │ (REST-based)      │
                                   └───────────────────┘
```

### Connection Abstraction (`singlestoredb/connection.py`)

The high-level `Connection` and `Cursor` classes in `connection.py` define the abstract
interface that both protocol implementations must satisfy.

**Connection provides:**

- **DB-API 2.0 compliance**: `connect()`, `cursor()`, `commit()`, `rollback()`, `close()`
- **Context manager support**: `with s2.connect(...) as conn:`
- **Variable accessor**: `conn.locals.autocommit = True`
- **Show accessor**: `conn.show.tables()`, `conn.show.databases()`

**Cursor provides:**

- **DB-API 2.0 methods**: `execute()`, `executemany()`, `fetchone()`, `fetchmany()`, `fetchall()`
- **Iteration support**: `for row in cursor:`
- **Context manager**: `with conn.cursor() as cur:`
- **Result metadata**: `description`, `rowcount`, `rownumber`

#### ShowAccessor

The `ShowAccessor` class provides convenient methods for `SHOW` commands:

```python
conn.show.tables()           # SHOW TABLES
conn.show.databases()        # SHOW DATABASES
conn.show.columns('users')   # SHOW COLUMNS FROM users
conn.show.processlist()      # SHOW PROCESSLIST
conn.show.create_table('t')  # SHOW CREATE TABLE t
```

#### VariableAccessor

The `VariableAccessor` class (`conn.locals` / `conn.globals`) provides attribute-style
access to session and global variables:

```python
conn.locals.autocommit = True       # SET autocommit = 1
print(conn.locals.autocommit)       # SELECT @@autocommit
conn.globals.max_connections        # SELECT @@global.max_connections
```

### MySQL Connector (`singlestoredb/mysql/`)

The MySQL connector is based on PyMySQL but heavily extended for SingleStore features.

#### SingleStore-Specific Features

The connector includes extensive SingleStore-specific functionality beyond standard MySQL:

**Vector Data Types:**
```python
# Native support for SingleStore vector types
conn = s2.connect(..., enable_extended_data_types=True, vector_data_format='binary')

cur.execute('''
    CREATE TABLE embeddings (
        id INT,
        vec VECTOR(384, F32)  -- 384-dimensional float32 vector
    )
''')

# Vectors returned as numpy arrays (with binary format) or lists (with JSON format)
cur.execute('SELECT vec FROM embeddings WHERE id = 1')
vector = cur.fetchone()[0]  # numpy.ndarray with shape (384,)
```

**Extended Data Types:**
- `VECTOR(n, F32/F64/I8/I16/I32/I64)` - Dense vector types for ML embeddings
- `BSON` - Binary JSON for document storage
- Automatic conversion between Python types and SingleStore extended types

**SingleStore SHOW Commands** (via `ShowAccessor`):
```python
conn.show.pipelines()       # SHOW PIPELINES - SingleStore data ingestion
conn.show.partitions()      # SHOW PARTITIONS - distributed table info
conn.show.plancache()       # SHOW PLANCACHE - query plan cache
conn.show.aggregates()      # SHOW AGGREGATES - user-defined aggregates
conn.show.reproduction()    # SHOW REPRODUCTION - query optimizer debug info
conn.show.plan(plan_id)     # SHOW PLAN <id> - execution plan details
```

**Fusion SQL Integration:**
- Client-side interception of extended SQL commands
- Workspace management via SQL syntax
- Stage (file storage) operations via SQL

**Multiple Result Formats:**

The SDK supports 7 result formats, selectable via `results_type` parameter:

```python
# Tuples (default) - standard DB-API
conn.cursor(results_type='tuples')
rows = cur.fetchall()  # [(1, 'alice'), (2, 'bob')]

# Dictionaries - column names as keys
conn.cursor(results_type='dicts')
rows = cur.fetchall()  # [{'id': 1, 'name': 'alice'}, ...]

# Named tuples - attribute access
conn.cursor(results_type='namedtuples')
row = cur.fetchone()
print(row.name)  # 'alice'

# NumPy - structured arrays for numerical computing
conn.cursor(results_type='numpy')
arr = cur.fetchall()  # numpy structured array

# Pandas - DataFrames for data analysis
conn.cursor(results_type='pandas')
df = cur.fetchall()  # pandas.DataFrame

# Polars - DataFrames for fast analytics
conn.cursor(results_type='polars')
df = cur.fetchall()  # polars.DataFrame

# Arrow - columnar format for zero-copy interchange
conn.cursor(results_type='arrow')
table = cur.fetchall()  # pyarrow.Table
```

Each format also has streaming variants (`SS*Cursor`) for memory-efficient iteration
over large result sets without buffering the entire result in memory.

#### Key Files

| File | Purpose |
|------|---------|
| `connection.py` | MySQL connection with C acceleration support |
| `cursors.py` | 20+ cursor implementations using mixin composition |
| `protocol.py` | MySQL wire protocol parsing |
| `_auth.py` | Authentication methods (native, SHA256, GSSAPI) |
| `converters.py` | MySQL data type converters |
| `charset.py` | Character set definitions |

#### C Acceleration

The C extension (`accel.c` → `_singlestoredb_accel.abi3.so`) provides ~10x faster row
parsing. It's automatically used unless disabled:

```bash
# Force pure Python mode
export SINGLESTOREDB_PURE_PYTHON=1
```

The acceleration targets the hot path in row parsing from the binary protocol.

#### Cursor Hierarchy

The SDK provides 20+ cursor types through mixin composition. The pattern allows combining
buffering behavior with result formats:

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Base Cursors                                │
├─────────────────────────────────────────────────────────────────────┤
│  Cursor (buffered)              SSCursor (server-side/streaming)    │
│  CursorSV (substitution vars)   SSCursorSV                          │
└─────────────────────────────────────────────────────────────────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              │                    │                    │
              ▼                    ▼                    ▼
┌───────────────────────┐ ┌───────────────────────┐ ┌───────────────────────┐
│    Format Mixins      │ │    Result Cursors     │ │  Streaming Variants   │
├───────────────────────┤ ├───────────────────────┤ ├───────────────────────┤
│ DictCursorMixin       │ │ DictCursor            │ │ SSDictCursor          │
│ NamedtupleCursorMixin │ │ NamedtupleCursor      │ │ SSNamedtupleCursor    │
│ ArrowCursorMixin      │ │ ArrowCursor           │ │ SSArrowCursor         │
│ NumpyCursorMixin      │ │ NumpyCursor           │ │ SSNumpyCursor         │
│ PandasCursorMixin     │ │ PandasCursor          │ │ SSPandasCursor        │
│ PolarsCursorMixin     │ │ PolarsCursor          │ │ SSPolarsCursor        │
└───────────────────────┘ └───────────────────────┘ └───────────────────────┘
```

**Usage:**

```python
# Buffered dict cursor (loads all rows into memory)
with conn.cursor(results_type='dicts') as cur:
    cur.execute('SELECT * FROM users')
    rows = cur.fetchall()  # List of dicts

# Streaming pandas cursor (memory-efficient for large results)
with conn.cursor(results_type='pandas', buffered=False) as cur:
    cur.execute('SELECT * FROM large_table')
    for batch in cur.fetchall_unbuffered():  # Yields DataFrames
        process(batch)
```

#### Authentication Methods

Defined in `singlestoredb/mysql/_auth.py`:

| Method | Function | Description |
|--------|----------|-------------|
| Native | `scramble_native_password()` | MySQL native password authentication |
| SHA256 | `sha256_password_auth()` | SHA-256 with RSA encryption |
| Caching SHA2 | `caching_sha2_password_auth()` | Fast cached authentication |
| Ed25519 | `ed25519_password()` | Ed25519 signature authentication |
| GSSAPI | `gssapi_auth()` | Kerberos/GSSAPI authentication |

### HTTP Connector (`singlestoredb/http/`)

The HTTP connector provides REST-based access via SingleStore's HTTP API (port 9000).

**Key Characteristics:**
- JSON request/response encoding
- Simpler protocol, no binary parsing
- Useful for environments where MySQL protocol is blocked
- Same cursor interface as MySQL connector

```python
# Connect via HTTP API
conn = s2.connect('http://user:password@host:9000/database')

# Use exactly like MySQL connection
with conn.cursor() as cur:
    cur.execute('SELECT * FROM users')
    rows = cur.fetchall()
```

---

## Configuration System

The configuration system in `singlestoredb/config.py` provides centralized option
management with validation and environment variable support.

### Option Access

```python
import singlestoredb as s2

# Get current value
value = s2.options.get('results_type')

# Set value
s2.options.set('results_type', 'pandas')

# Context manager for temporary changes
with s2.options.option_context(results_type='arrow'):
    # Uses arrow format within this block
    conn = s2.connect(...)
```

### Key Configuration Categories

| Category | Options | Description |
|----------|---------|-------------|
| Connection | `host`, `user`, `password`, `port`, `database` | Database connection parameters |
| Performance | `pure_python`, `buffered` | Performance tuning |
| Results | `results_type`, `nan_as_null`, `inf_as_null` | Result handling |
| SSL | `ssl_key`, `ssl_cert`, `ssl_ca`, `ssl_disabled` | SSL/TLS configuration |
| Fusion | `fusion.enabled` | Fusion SQL enablement |

### Environment Variables

All options can be set via environment variables with the `SINGLESTOREDB_` prefix:

```bash
export SINGLESTOREDB_URL='user:password@host:3306/db'
export SINGLESTOREDB_RESULTS_TYPE='pandas'
export SINGLESTOREDB_PURE_PYTHON=1
export SINGLESTOREDB_FUSION_ENABLED=1
```

---

## Management API

The management API (`singlestoredb/management/`) provides programmatic access to
SingleStore's cloud management features.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                      api.singlestore.com                            │
└─────────────────────────────────────────────────────────────────────┘
                                   ▲
                                   │ REST + JWT Auth
                                   │
┌─────────────────────────────────────────────────────────────────────┐
│                          Manager (base)                             │
│                       singlestoredb/management/manager.py           │
│  _get(), _post(), _put(), _delete(), _patch()                       │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        WorkspaceManager                             │
│                    singlestoredb/management/workspace.py            │
├─────────────────────────────────────────────────────────────────────┤
│  workspace_groups()    regions()           organizations()          │
│  create_workspace()    get_workspace()     billing()                │
│  starter_workspaces()  create_workspace_group()                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Resource Hierarchy

```
Organization
    │
    ├── WorkspaceGroup
    │       ├── Stage (file storage)
    │       └── Workspace (database instance)
    │               └── connect() → Connection
    │
    └── StarterWorkspace (free tier)
            └── connect() → Connection
```

### Usage

```python
import singlestoredb as s2

# Initialize manager with API token
mgr = s2.manage_workspaces()

# List workspace groups
for wg in mgr.workspace_groups():
    print(wg.name, wg.id)

# Create a workspace
ws = mgr.create_workspace(
    name='my-workspace',
    workspace_group=wg,
    size='S-00',
)

# Connect to workspace
conn = ws.connect()

# Stage operations (file storage)
stage = wg.stage
stage.upload_file('local.csv', '/data/uploaded.csv')
stage.download_file('/data/uploaded.csv', 'downloaded.csv')
stage.listdir('/data')
```

### Key Classes

| Class | File | Purpose |
|-------|------|---------|
| `Manager` | `manager.py` | Base REST client with auth |
| `WorkspaceManager` | `workspace.py` | Main management interface |
| `WorkspaceGroup` | `workspace.py` | Group of workspaces |
| `Workspace` | `workspace.py` | Database instance |
| `Stage` | `workspace.py` | File storage operations |
| `StarterWorkspace` | `workspace.py` | Free tier workspace |
| `Organization` | `organization.py` | Organization management |
| `Billing` | `workspace.py` | Usage and billing |

---

## Fusion SQL

Fusion SQL extends the SQL interface with client-side command handling. Commands are
intercepted before being sent to the database and processed by registered handlers.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    cursor.execute(sql)                              │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
                    ┌─────────────────────────────────────┐
                    │       Fusion SQL Registry           │
                    │  singlestoredb/fusion/registry.py   │
                    │       Match leading keywords        │
                    └─────────────────────────────────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              │                    │                    │
              ▼                    ▼                    ▼
      ┌───────────┐        ┌───────────┐        ┌───────────┐
      │  No Match │        │  Handler  │        │  Handler  │
      │  → Send   │        │  Found    │        │  Found    │
      │  to DB    │        │           │        │           │
      └───────────┘        └───────────┘        └───────────┘
                                   │                    │
                                   ▼                    ▼
                           ┌─────────────────────────────────┐
                           │   SQLHandler.execute(sql)       │
                           │   Parse grammar → params dict   │
                           │   Call run(params)              │
                           │   Return FusionSQLResult        │
                           └─────────────────────────────────┘
```

### Enabling Fusion SQL

```python
import os
os.environ['SINGLESTOREDB_FUSION_ENABLED'] = '1'

import singlestoredb as s2
conn = s2.connect(...)

# Now Fusion commands work
cur.execute('SHOW WORKSPACE GROUPS')
```

### Handler Architecture

Handlers extend `SQLHandler` (`singlestoredb/fusion/handler.py`) with grammar defined
in the docstring:

```python
from singlestoredb.fusion.handler import SQLHandler
from singlestoredb.fusion import result

class MyHandler(SQLHandler):
    """
    SHOW MY DATA IN directory [ <extended> ];

    # Location of data
    directory = DIRECTORY '<path>'
    """

    def run(self, params):
        res = result.FusionSQLResult()
        res.add_field('Name', result.STRING)
        # ... populate results ...
        return res

MyHandler.register()
```

### Grammar Syntax

| Element | Syntax | Example |
|---------|--------|---------|
| Keywords | ALL_CAPS | `SHOW`, `CREATE`, `IN` |
| String literals | `'<name>'` | `'<path>'`, `'<region-id>'` |
| Numeric literals | `<number>`, `<integer>` | `<integer>` |
| Optional blocks | `[ ... ]` | `[ WITH PASSWORD ]` |
| Selection blocks | `{ A \| B }` | `{ QUIET \| VERBOSE }` |
| Repeated values | `rule,...` | `'<ip-range>',...` |
| Builtins | `<name>` | `<extended>`, `<like>`, `<limit>`, `<order-by>` |

### Built-in Handlers

Located in `singlestoredb/fusion/handlers/`:

| Handler | Commands |
|---------|----------|
| `workspace.py` | `SHOW WORKSPACE GROUPS`, `CREATE WORKSPACE`, etc. |
| `stage.py` | `UPLOAD`, `DOWNLOAD`, `CREATE STAGE FOLDER` |
| `job.py` | `SHOW JOBS`, `CREATE JOB`, `DROP JOB` |
| `files.py` | File management commands |
| `models.py` | ML model commands |
| `export.py` | Data export commands |

---

## External Functions (UDFs)

The functions module (`singlestoredb/functions/`) enables deploying Python functions
as SingleStore external functions.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        @udf Decorator                               │
│                 singlestoredb/functions/decorator.py                │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Signature Analysis                               │
│               singlestoredb/functions/signature.py                  │
│  Python types → SQL types, parameter specs, return specs            │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Execution Modes                                 │
│                 singlestoredb/functions/ext/                        │
├─────────────────────────────────────────────────────────────────────┤
│  asgi.py    │  HTTP server via ASGI (Uvicorn)                      │
│  mmap.py    │  Memory-mapped shared memory (collocated)            │
│  json.py    │  JSON serialization over HTTP                        │
│  rowdat_1.py│  ROWDAT_1 binary format                              │
│  arrow.py   │  Apache Arrow columnar format                        │
└─────────────────────────────────────────────────────────────────────┘
```

### Usage

```python
# my_functions.py
from singlestoredb.functions import udf

@udf
def add_numbers(a: int, b: int) -> int:
    return a + b

@udf
def process_text(text: str) -> str:
    return text.upper()
```

```bash
# Deploy as HTTP server
python -m singlestoredb.functions.ext.asgi \
    --host 0.0.0.0 \
    --port 8000 \
    --db 'user:pass@host/db' \
    my_functions
```

### Type Mapping

The `signature.py` module maps Python types to SQL types:

| Python Type | SQL Type |
|-------------|----------|
| `int` | `BIGINT` |
| `float` | `DOUBLE` |
| `str` | `TEXT` |
| `bytes` | `BLOB` |
| `bool` | `TINYINT` |
| `datetime.datetime` | `DATETIME` |
| `datetime.date` | `DATE` |
| `decimal.Decimal` | `DECIMAL` |
| `List[T]` | `ARRAY` |
| `Optional[T]` | Nullable variant |

### Execution Modes

```
┌─────────────────────────────────────────────────────────────────────┐
│                      SingleStore Database                           │
└─────────────────────────────────────────────────────────────────────┘
         │                    │                    │
         │ ASGI/HTTP          │ Memory-mapped      │ JSON
         │ (remote)           │ (collocated)       │ (simple)
         ▼                    ▼                    ▼
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   asgi.py   │      │   mmap.py   │      │   json.py   │
│  Uvicorn    │      │  Shared     │      │  Simple     │
│  HTTP/2     │      │  Memory     │      │  Serialize  │
└─────────────┘      └─────────────┘      └─────────────┘
         │                    │                    │
         └────────────────────┼────────────────────┘
                              ▼
                    ┌─────────────────┐
                    │  Python UDF     │
                    │  Functions      │
                    └─────────────────┘
```

| Mode | File | Use Case |
|------|------|----------|
| ASGI | `asgi.py` | Remote execution via HTTP, scalable |
| Memory-mapped | `mmap.py` | Collocated execution, lowest latency |
| JSON | `json.py` | Simple serialization, debugging |
| ROWDAT_1 | `rowdat_1.py` | Binary format, efficient |
| Arrow | `arrow.py` | Columnar format, analytics |

---

## AI Integration

The AI module (`singlestoredb/ai/`) provides factory functions for creating AI clients
configured for SingleStore's AI services.

### Chat Factory

```python
from singlestoredb.ai import SingleStoreChatFactory

chat = SingleStoreChatFactory(
    model='gpt-4',
    provider='openai',
)

response = chat.complete('What is SingleStore?')
```

### Embeddings Factory

```python
from singlestoredb.ai import SingleStoreEmbeddingsFactory

embeddings = SingleStoreEmbeddingsFactory(
    model='text-embedding-ada-002',
    provider='openai',
)

vectors = embeddings.embed(['Hello', 'World'])
```

### Supported Providers

- **OpenAI**: GPT models, text embeddings
- **AWS Bedrock**: Claude, Titan models

---

## Vector Store

The vector store module (`singlestoredb/vectorstore.py`) integrates with the
`singlestore-vectorstore` package for vector similarity search.

```python
import singlestoredb as s2

# Create vector store from connection
conn = s2.connect(...)
vs = conn.vector_db(
    table_name='embeddings',
    vector_column='embedding',
)

# Or use standalone
from singlestoredb import vector_db
vs = vector_db(
    host='localhost',
    table_name='embeddings',
)
```

---

## Supporting Modules

### Types (`singlestoredb/types.py`)

DB-API 2.0 type objects for parameter binding:

```python
from singlestoredb import STRING, BINARY, NUMBER, DATETIME, ROWID
```

### Converters (`singlestoredb/converters.py`)

Data type conversion between Python and database types. Handles encoding/decoding of
dates, times, decimals, JSON, vectors, and binary data.

### Exceptions (`singlestoredb/exceptions.py`)

DB-API 2.0 exception hierarchy:

```
Error
├── InterfaceError
└── DatabaseError
    ├── DataError
    ├── OperationalError
    ├── IntegrityError
    ├── InternalError
    ├── ProgrammingError
    └── NotSupportedError

ManagementError (separate hierarchy for management API)
```

### Authentication (`singlestoredb/auth.py`)

JWT token handling for management API authentication:

```python
from singlestoredb import auth

# Credential types
auth.PASSWORD      # Password authentication
auth.JWT           # JWT token authentication
auth.BROWSER_SSO   # Browser-based SSO
```

---

## Testing Infrastructure

The testing infrastructure (`singlestoredb/pytest.py`) provides automatic Docker
container management for tests.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        pytest_configure                             │
└─────────────────────────────────────────────────────────────────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              │                    │                    │
              ▼                    ▼                    ▼
   ┌───────────────────┐  ┌───────────────────┐  ┌───────────────────┐
   │ SINGLESTOREDB_URL │  │ No URL set        │  │ USE_DATA_API=1    │
   │ is set            │  │ → Start Docker    │  │ → Use HTTP port   │
   └───────────────────┘  └───────────────────┘  └───────────────────┘
              │                    │                    │
              ▼                    ▼                    ▼
   ┌───────────────────┐  ┌───────────────────────┐  ┌───────────────────┐
   │ Use existing      │  │ _TestContainerManager │  │ Set HTTP URL      │
   │ server            │  │ Start container       │  │ for tests         │
   └───────────────────┘  └───────────────────────┘  └───────────────────┘
```

### _TestContainerManager

The `_TestContainerManager` class (`singlestoredb/pytest.py`) handles:

- Starting SingleStore Docker container
- Dynamic port allocation (avoids conflicts)
- Connection health checks
- Container cleanup

```python
class _TestContainerManager:
    def start(self) -> None: ...
    def connect(self) -> Connection: ...
    def stop(self) -> None: ...
    def http_connection_url(self) -> str: ...
```

### Fixtures

| Fixture | Purpose |
|---------|---------|
| `singlestoredb_test_container` | Manages Docker container lifecycle |
| `singlestoredb_connection` | Provides a connection to test server |
| `singlestoredb_tempdb` | Creates temporary database with cursor |
| `execution_mode` | Returns MySQL or HTTP mode |
| `name_allocator` | Generates unique test names |

### Test Categories

| Category | Marker | Description |
|----------|--------|-------------|
| Protocol | (none) | MySQL protocol tests |
| HTTP | `USE_DATA_API=1` | HTTP API tests |
| Management | `@pytest.mark.management` | Management API tests |
| Pure Python | `SINGLESTOREDB_PURE_PYTHON=1` | No C acceleration |

---

## Additional Integrations

### SQLAlchemy (`singlestoredb/alchemy/`)

SQLAlchemy dialect for SingleStore:

```python
from sqlalchemy import create_engine

engine = create_engine('singlestoredb://user:pass@host/db')
```

### Jupyter/Notebook (`singlestoredb/notebook/`, `singlestoredb/magics/`)

IPython magic commands for notebooks:

```python
%load_ext singlestoredb

%%sql
SELECT * FROM users
```

#### Portal and Live Accessor Objects

When running in SingleStore Helios notebooks, the `singlestoredb.notebook` module
provides live accessor objects that automatically reflect the currently selected
cloud resources in the Portal UI:

```python
from singlestoredb.notebook import portal

# Access current context (automatically synced with Portal UI)
portal.organization       # Current Organization object
portal.workspace_group    # Current WorkspaceGroup object
portal.workspace          # Current Workspace object
portal.stage              # Current Stage (file storage) object
portal.secrets            # Secrets accessor

# Connection details from Portal
portal.host               # Database hostname
portal.port               # Database port
portal.user               # Username
portal.password           # Password
portal.default_database   # Selected database
portal.connection_url     # Full connection URL
```

**Secrets Accessor** - attribute-style access to stored secrets:
```python
from singlestoredb.notebook import secrets

api_key = secrets.OPENAI_API_KEY      # Access secret by name
api_key = secrets['OPENAI_API_KEY']   # Dictionary-style also works
```

**Stage Accessor** - proxy to the currently selected Stage:
```python
from singlestoredb.notebook import stage

stage.upload_file('local.csv', '/data/uploaded.csv')
stage.download_file('/data/file.csv', 'local.csv')
files = stage.listdir('/data')
```

**Changing Resources** - updates Portal UI via JavaScript bridge:
```python
# Switch workspace (updates Portal UI)
portal.workspace = 'workspace-name'
portal.workspace = 'workspace-uuid'

# Switch workspace and database
portal.connection = ('workspace-name', 'database-name')

# Change default database
portal.default_database = 'new_database'
```

The accessors are "live" - they always reflect the current Portal state and changes
propagate bidirectionally between Python and the Portal UI.

### Server Tools (`singlestoredb/server/`)

Utilities for managing SingleStore server instances, including Docker helpers and
free tier management.

---

## Appendices

### A. Environment Variables Reference

| Variable | Description | Default |
|----------|-------------|---------|
| `SINGLESTOREDB_URL` | Full connection URL | None |
| `SINGLESTOREDB_HOST` | Database host | localhost |
| `SINGLESTOREDB_PORT` | Database port | 3306 |
| `SINGLESTOREDB_USER` | Database user | root |
| `SINGLESTOREDB_PASSWORD` | Database password | None |
| `SINGLESTOREDB_DATABASE` | Default database | None |
| `SINGLESTOREDB_PURE_PYTHON` | Disable C acceleration | 0 |
| `SINGLESTOREDB_RESULTS_TYPE` | Default result format | tuples |
| `SINGLESTOREDB_FUSION_ENABLED` | Enable Fusion SQL | 0 |
| `SINGLESTOREDB_MANAGEMENT_TOKEN` | Management API token | None |
| `SINGLESTORE_LICENSE` | License key for Docker | None |
| `USE_DATA_API` | Use HTTP API for tests | 0 |

### B. Cursor Type Matrix

| Cursor | Buffered | Format | Class |
|--------|----------|--------|-------|
| Default | Yes | tuples | `Cursor` |
| Dict | Yes | dict | `DictCursor` |
| Namedtuple | Yes | namedtuple | `NamedtupleCursor` |
| Arrow | Yes | PyArrow | `ArrowCursor` |
| Numpy | Yes | numpy arrays | `NumpyCursor` |
| Pandas | Yes | DataFrame | `PandasCursor` |
| Polars | Yes | DataFrame | `PolarsCursor` |
| SS* | No | (streaming) | `SS*Cursor` |

### C. Configuration Options Reference

Connection options:
- `host`, `user`, `password`, `port`, `database`
- `driver`: `mysql`, `http`, `https`
- `charset`: Character encoding
- `ssl_*`: SSL/TLS options
- `credential_type`: `PASSWORD`, `JWT`, `BROWSER_SSO`

Performance options:
- `pure_python`: Disable C acceleration
- `buffered`: Buffer all results in memory
- `connect_timeout`: Connection timeout in seconds

Result options:
- `results_type`: `tuples`, `dicts`, `namedtuples`, `numpy`, `pandas`, `polars`, `arrow`
- `nan_as_null`: Treat NaN as NULL
- `inf_as_null`: Treat Inf as NULL

Feature options:
- `local_infile`: Allow local file uploads
- `multi_statements`: Allow multiple statements
- `autocommit`: Auto-commit mode
- `enable_extended_data_types`: Enable BSON, vector types
- `vector_data_format`: `json` or `binary`

### D. File Quick Reference

| Purpose | Primary File |
|---------|-------------|
| Entry point | `singlestoredb/__init__.py` |
| Connect function | `singlestoredb/connection.py:1312` |
| MySQL connection | `singlestoredb/mysql/connection.py` |
| Cursor types | `singlestoredb/mysql/cursors.py` |
| HTTP connection | `singlestoredb/http/connection.py` |
| Configuration | `singlestoredb/config.py` |
| Management API | `singlestoredb/management/workspace.py` |
| Fusion handlers | `singlestoredb/fusion/handler.py` |
| UDF decorator | `singlestoredb/functions/decorator.py` |
| Test fixtures | `singlestoredb/pytest.py` |
