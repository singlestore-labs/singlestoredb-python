# Release process

1. Bump the version number in `setup.cfg` and `singlestoredb/__init__.py` using
   semantic versioning rules: minor bump for new features, patch bump for
   bug fixes.

2. Add release notes to `docs/src/whatsnew.rst`.

3. Run `SINGLESTOREDB_URL=root:@db-server:db-port make html` in `docs/src` to
   generate documentation. You will need `sphinx` and `sphinx_rtd_theme` installed
   for this step. You also need a SingleStoreDB server running at the given
   IP and port to run samples against.

4. Commit all changed files with a commit like "Prepare for vX.X.X release".

5. The coverage tests will be triggered by the push, but you should also run
   `Smoke test` workflow manually which does basic tests on all supported versions
   of Python.

6. Once all workflows are clean, create a new Github release with the name
   "SingleStoreDB vX.X.X" and set the generated tag to the matching version
   number. Add the release notes from the `whatsnew.rst` file to the release
   notes. Creating the release will run the `Public packages` workflow which
   builds the packages and pubsishes them to PyPI.
