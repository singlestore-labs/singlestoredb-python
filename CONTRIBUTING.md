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

To create a test environment, do the following:
```
pip install -e ".[dev]"
```

Or if you only need specific dependency groups:
```
pip install -e ".[test]"  # Just testing dependencies
pip install -e ".[docs]"  # Just documentation dependencies
```

If you have Docker installed, you can run the tests as follows. Note that
you should run the tests using both standard protocol and Data API (HTTP):
```
pytest -v singlestoredb/tests
USE_DATA_API=1 -v singlestoredb/tests
```

If you need to run against a specific server version, you can specify
the URL of that server:
```
SINGLESTOREDB_URL=user:pw@127.0.0.1:3306 pytest -v singlestoredb/tests
SINGLESTOREDB_URL=http://user:pw@127.0.0.1:8090 pytest -v singlestoredb/tests
```
