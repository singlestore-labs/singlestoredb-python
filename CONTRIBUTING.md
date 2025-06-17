# SingleStore Python SDK Contributing Guide

Fork this repo and commit your changes to the forked repo.
From there make a Pull Request with your submission keeping the following in mind:

## Pre-commit checks on the clone of this repo

The CI pipeline in this repo runs a bunch of validation checks and code reformatting with pre-commit checks. If you don't install those checks in your clone of the repo, the code will likely not pass. To install the pre-commit tool in your clone run the following from your clone directory. This will force the checks before you can push.

```bash
pip3 install pre-commit==3.7.1
pre-commit install
```

The checks run automatically when you attempt to commit, but you can run them manually as well with the following:
```bash
pre-commit run --all-files
```
