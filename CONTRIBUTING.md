# SingleStore Python SDK Contributing Guide

Fork this repo and commit your changes to the forked repo.
From there make a Pull Request with your submission keeping the
following in mind:

## Setting up a development environment

Use [uv](https://docs.astral.sh/uv/) to create a virtual environment and install
development dependencies:

```bash
# Create and activate a virtual environment
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install the package with all development dependencies
uv pip install -e ".[dev]"

# Install pre-commit hooks (required for contributions)
pre-commit install
```

Alternatively, use standard pip:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pre-commit install
```

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
- **Development environment set up** (see above)
- **Docker installed and running** (for automatic test database management)

The `docker` Python package (included in `[dev]` and `[test]` extras) is required
for the test framework to manage Docker containers automatically.

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

- **`SINGLESTOREDB_MANAGEMENT_TOKEN`**: Management API token for testing management features. Get your API key from the [SingleStore Portal](https://portal.singlestore.com/). Tests marked with `@pytest.mark.management` will create and delete workspace groups/workspaces in your organization.

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
   - Set `SINGLESTOREDB_MANAGEMENT_TOKEN` to your API key
   - **Warning**: These tests create actual workspace groups and workspaces in the SingleStore Launchpad organization associated with your API key
   - Resources are cleaned up after tests, but failed tests may leave orphaned resources
   - To skip management tests: `pytest -v -m 'not management' singlestoredb/tests`

### Examples

```bash
# Standard workflow - test both protocols
pytest -v singlestoredb/tests
USE_DATA_API=1 pytest -v singlestoredb/tests

# Test single module with coverage
pytest -v --cov=singlestoredb.connection singlestoredb/tests/test_connection.py

# Test UDF functionality
pytest singlestoredb/tests/test_udf.py

# Manual testing of the plugin UDF server
python -m singlestoredb.functions.ext.plugin \
    --plugin-name myfuncs --search-path /path/to/modules

# Test against specific server (skips Docker)
SINGLESTOREDB_URL=admin:pass@localhost:3306 pytest -v singlestoredb/tests

# Debug mode with verbose output
pytest -vv -s singlestoredb/tests/test_basics.py
```

## Version Bumping and Releases

This section documents the process for creating new releases of the SDK.

### Bumping the Version

Use the `resources/bump_version.py` script to increment the version number
and prepare release notes.

```bash
# Bump patch version (1.2.3 -> 1.2.4)
python resources/bump_version.py patch

# Bump minor version (1.2.3 -> 1.3.0)
python resources/bump_version.py minor

# Bump major version (1.2.3 -> 2.0.0)
python resources/bump_version.py major

# Provide custom release notes (supports reStructuredText)
python resources/bump_version.py patch --summary "* Fixed critical bug in connection handling"
```

The script performs the following steps:
1. Reads the current version from `pyproject.toml`
2. Calculates the new version based on bump type
3. Updates version in both `pyproject.toml` and `singlestoredb/__init__.py`
4. Generates release notes from git history (or uses provided summary)
5. Opens an editor to customize release notes for `docs/src/whatsnew.rst`
6. Adds the new release to `COMPATIBILITY.md`
7. Builds the documentation
8. Stages all modified files for commit
9. Optionally commits to a `release/v<version>` branch, pushes it, and opens a
   pull request against `main`

Changes to `main` must go through a reviewed pull request, so before touching
any files the script checks that you are on `main`, that tracked files are
clean, and that local `main` matches `origin/main`. It also stops if the
`release/v<version>` branch already exists.

If `COMPATIBILITY.md` cannot be generated, for example without network access, the script logs a warning and continues the release. Run `python resources/generate_compatibility.py` afterwards to add the missing row.

`COMPATIBILITY.md` is generated from the release dates in `docs/src/whatsnew.rst`, the Python matrices in each release tag's smoke-test workflow, and the [SingleStore software EOL policy](https://docs.singlestore.com/db/latest/support/singlestore-software-end-of-life-eol-policy/). An engine is listed when it had been released and its end-of-life date was still in the future on that client release date. The [latest release candidate](https://release.memsql.com/rc/index/singlestoredbserver/latest.json) is added only to a release whose tag does not exist yet, which is the release being prepared.

Rows already in `COMPATIBILITY.md` are kept as they are; only releases missing from the file are generated, and only those need network access. To add missing rows, check that the file is current, or regenerate every row, run:

```bash
python resources/generate_compatibility.py
python resources/generate_compatibility.py --verify-matrix
python resources/generate_compatibility.py --rebuild
```

### Creating a Release

After the release pull request is merged, use the `resources/create_release.py`
script to create a GitHub release.

```bash
# Create release using the version on origin/main
python resources/create_release.py

# Create release, checking origin/main is at a specific version
python resources/create_release.py --version 1.16.9

# Preview without creating (dry run)
python resources/create_release.py --dry-run
```

The script:
1. Checks prerequisites (gh CLI installed, authenticated with GitHub)
2. Fetches `origin/main` and reads the version from its `pyproject.toml`
   (`--version` must match it), and stops if tag `v<version>` already exists
3. Reads release notes from `docs/src/whatsnew.rst` on `origin/main`
4. Creates a GitHub release with tag `v<version>` on that `origin/main` commit

The version and notes are read from `origin/main` rather than your working tree,
so running the script before the release pull request is merged fails with a
version mismatch instead of tagging the previous release's code.

**Prerequisites**: The GitHub CLI (`gh`) must be installed and authenticated.
Install from https://cli.github.com/ and run `gh auth login`.

### Complete Release Workflow

1. **Bump the version**:
   ```bash
   python resources/bump_version.py patch
   ```

2. **Review and edit release notes** in the editor that opens

3. **Open the release pull request** (the script prompts for this, or do manually):
   ```bash
   git switch -c release/v1.x.x
   git commit -m "Prepare for v1.x.x release"
   git push -u origin release/v1.x.x
   gh pr create --base main --fill
   git switch main
   ```

4. **Wait for CI tests to pass** on the pull request, and check the nightly runs:
   - [Coverage tests](https://github.com/singlestore-labs/singlestoredb-python/actions/workflows/coverage.yml)
   - [Smoke tests](https://github.com/singlestore-labs/singlestoredb-python/actions/workflows/smoke-test.yml)

5. **Get the pull request approved by a code owner and merged**, then update
   your local `main`:
   ```bash
   git pull --ff-only
   ```

6. **Create the GitHub release**:
   ```bash
   python resources/create_release.py
   ```

7. **Verify [PyPI publish](https://github.com/singlestore-labs/singlestoredb-python/actions/workflows/publish.yml)**
   workflow completes successfully (triggered automatically by the GitHub release).
   Check the [PyPI package page](https://pypi.org/project/singlestoredb/) to confirm
   the new version is available.

### Version File Locations

Version numbers are stored in two locations that must stay in sync:
- `pyproject.toml` (line 7): Package metadata version
- `singlestoredb/__init__.py` (line 16): `__version__` variable

The `bump_version.py` script handles updating both files automatically.
Never edit these manually unless you update both locations.
