# SingleStore Python SDK Contributing Guide

Fork this repo and commit your changes to the forked repo.
From there make a Pull Request with your submission keeping the
following in mind:

## Pre-commit checks on the clone of this repo

The CI pipeline in this repo runs a bunch of validation checks and code
reformatting with pre-commit checks. If you don't install those checks
in your clone of the repo, the code will likely not pass. To install
the pre-commit tool in your clone run the following from your clone
directory. This will force the checks before you can push.

```
pip install pre-commit==3.7.1
pre-commit install
```

The checks run automatically when you attempt to commit, but you can run
them manually as well with the following:
```
pre-commit run --all-files
```

## Running tests

### Prerequisites

Before running tests, ensure you have:
- **Docker installed and running** (for automatic test database management)
- Test dependencies installed: `pip install -e ".[test]"` or `pip install -e ".[dev]"` for all development dependencies

The `docker` Python package is required for the test framework to manage Docker containers automatically.

### Installation

To create a test environment:
```bash
pip install -e ".[dev]"  # All development dependencies (recommended)
```

Or if you only need specific dependency groups:
```bash
pip install -e ".[test]"      # Just testing dependencies
pip install -e ".[docs]"      # Just documentation dependencies
pip install -e ".[build]"     # Just build dependencies
pip install -e ".[examples]"  # Just example/demo dependencies
```

### Basic Testing

The test framework provides **automatic Docker container management**. When you run tests without setting `SINGLESTOREDB_URL`, the framework will:

1. Automatically start a SingleStore Docker container (`ghcr.io/singlestore-labs/singlestoredb-dev`)
2. Allocate dynamic ports to avoid conflicts (MySQL, Data API, Studio)
3. Wait for the container to be ready
4. Run all tests against the container
5. Clean up the container after tests complete

#### Standard MySQL Protocol Tests
```bash
# Run all tests (auto-starts Docker container)
pytest -v singlestoredb/tests

# Run with coverage
pytest -v --cov=singlestoredb --pyargs singlestoredb.tests

# Run single test file
pytest singlestoredb/tests/test_basics.py

# Run without management API tests
pytest -v -m 'not management' singlestoredb/tests
```

#### Data API Tests

The SDK supports testing via SingleStore's **Data API** (port 9000) instead of the MySQL protocol (port 3306). This mode uses a **dual-URL system**:

- `SINGLESTOREDB_URL`: Set to HTTP Data API endpoint (port 9000) for test operations
- `SINGLESTOREDB_INIT_DB_URL`: Automatically set to MySQL endpoint (port 3306) for setup operations

**Why dual URLs?** Some setup operations like `SET GLOBAL` and `USE database` commands don't work over the HTTP Data API, so they're routed through the MySQL protocol automatically.

Enable HTTP Data API testing:
```bash
# Run tests via HTTP Data API
USE_DATA_API=1 pytest -v singlestoredb/tests
```

**Known Limitations in HTTP Data API Mode:**
- `USE database` command is not supported (some tests will be skipped)
- Setup operations requiring `SET GLOBAL` are automatically routed to MySQL protocol

#### Testing Against an Existing Server

If you have a running SingleStore instance, you can test against it by setting `SINGLESTOREDB_URL`. The Docker container will not be started.

```bash
# Test against MySQL protocol
SINGLESTOREDB_URL=user:password@host:3306 pytest -v singlestoredb/tests

# Test against Data API
SINGLESTOREDB_INIT_DB_URL=user:password@host:3306 \
    SINGLESTOREDB_URL=http://user:password@host:9000 \
    pytest -v singlestoredb/tests
```

### Docker Container Details

When the test framework starts a Docker container automatically:

- **Container name**: `singlestoredb-test-{worker}-{uuid}` (supports parallel test execution)
- **Port mappings**:
  - MySQL protocol: Random available port → Container port 3306
  - Data API (HTTP): Random available port → Container port 9000
  - Studio: Random available port → Container port 8080
- **License**: Uses `SINGLESTORE_LICENSE` environment variable if set, otherwise runs without license
- **Cleanup**: Container is automatically removed after tests complete

### Environment Variables

The following environment variables control test behavior:

- **`SINGLESTOREDB_URL`**: Database connection URL. If not set, a Docker container is started automatically.
  - MySQL format: `user:password@host:3306`
  - HTTP format: `http://user:password@host:9000`

- **`USE_DATA_API`**: Set to `1`, `true`, or `on` to run tests via HTTP Data API instead of MySQL protocol.
  - Automatically sets up the dual-URL system
  - Example: `USE_DATA_API=1 pytest -v singlestoredb/tests`

- **`SINGLESTOREDB_INIT_DB_URL`**: MySQL connection URL for setup operations (auto-set in HTTP Data API mode). Used for operations that require MySQL protocol even when testing via HTTP.

- **`SINGLESTORE_LICENSE`**: Optional license key for Docker container. If not provided, container runs without a license.

- **`SINGLESTOREDB_PURE_PYTHON`**: Set to `1` to disable C acceleration and test in pure Python mode.

- **`SINGLESTOREDB_MANAGEMENT_TOKEN`**: Management API token for testing management features (mark tests with `@pytest.mark.management`).

### Testing Best Practices

1. **Test both protocols**: Always run tests with both MySQL protocol and HTTP Data API before submitting:
   ```bash
   pytest -v singlestoredb/tests
   USE_DATA_API=1 pytest -v singlestoredb/tests
   ```

2. **Pure Python testing**: Test without C acceleration to ensure compatibility:
   ```bash
   SINGLESTOREDB_PURE_PYTHON=1 pytest -v singlestoredb/tests
   ```

3. **Management API tests**: These require a management token and are marked with `@pytest.mark.management`.

### Examples

```bash
# Standard workflow - test both protocols
pytest -v singlestoredb/tests
USE_DATA_API=1 pytest -v singlestoredb/tests

# Test single module with coverage
pytest -v --cov=singlestoredb.connection singlestoredb/tests/test_connection.py

# Test UDF functionality
pytest singlestoredb/tests/test_udf.py

# Test against specific server (skips Docker)
SINGLESTOREDB_URL=admin:pass@localhost:3306 pytest -v singlestoredb/tests

# Debug mode with verbose output
pytest -vv -s singlestoredb/tests/test_basics.py
```
