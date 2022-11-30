#!/bin/bash

if [[ $(uname) =~ Linux* ]]; then
    python -m pip install --no-deps --ignore-installed dist/singlestoredb-*linux*.whl
fi

if [[ $(uname) =~ Darwin* ]]; then
    python -m pip install --no-deps --ignore-installed dist/singlestoredb-*macos*.whl
fi
