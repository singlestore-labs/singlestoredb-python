# Release process

## Bump the package version and build documentation

Bump the version number in `setup.cfg` and `singlestoredb/__init__.py` using
semantic versioning rules: minor bump for new features, patch bump for
bug fixes. Add release notes to `docs/src/whatsnew.rst`. Run `make html` in
`docs/src` to generate documentation.

You will need `sphinx` and `sphinx_rtd_theme` installed for this step. You
also need a SingleStoreDB server running at the given IP and port to run
samples against.

There is a utility to do this process for you, but you should check the
`docs/src/whatsnew.rst` to verify the release summary. Use the following
to run it:
```
resources/bump_version.py < major | minor | patch >

```

## Commit and push the changes

After verifying the release summary in the documentation, commit the changes:
```
# Make sure newly generated docs get added
git add docs

# Commit changes
git commit -am "Prepare for vX.X.X release".

git push

```

## Run smoke tests

The coverage tests will be triggered by the push, but you should also run
[Smoke test](https://github.com/singlestore-labs/singlestoredb-python/actions/workflows/smoke-test.yml)
workflow manually which does basic tests on all supported versions of Python.

## Create the release on Github

Once all workflows are clean, create a new Github release with the name
"SingleStoreDB vX.X.X" at <https://github.com/singlestore-labs/singlestoredb-python/releases>
and set the generated tag to the matching version
number. Add the release notes from the `whatsnew.rst` file to the release
notes. Creating the release will run the [Publish packages](https://github.com/singlestore-labs/singlestoredb-python/actions/workflows/publish.yml)
workflow which builds the packages and pubsishes them to PyPI.
