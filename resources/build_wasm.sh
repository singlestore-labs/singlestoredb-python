#!/bin/bash

set -eou pipefail

# CPYTHON_ROOT must contain a build of cpython for wasm32-wasip2

TARGET="wasm32-wasip2"
CROSS_BUILD="${CPYTHON_ROOT}/cross-build/${TARGET}"
WASI_SDK_PATH=${WASI_SDK_PATH:-/opt/wasi-sdk}
PYTHON_VERSION=$(grep '^VERSION=' "${CROSS_BUILD}/Makefile" | sed 's/VERSION=[[:space:]]*//')

if [ ! -e wasm_venv ]; then
  uv venv --python ${PYTHON_VERSION} wasm_venv
fi

. wasm_venv/bin/activate

HOST_PYTHON=$(which python3)

uv pip install build wheel cython setuptools

ARCH_TRIPLET=_wasi_wasm32-wasi

export CC="${WASI_SDK_PATH}/bin/clang"
export CXX="${WASI_SDK_PATH}/bin/clang++"

export PYTHONPATH="${CROSS_BUILD}/build/lib.wasi-wasm32-${PYTHON_VERSION}"

export CFLAGS="--target=${TARGET} -fPIC -I${CROSS_BUILD}/install/include/python${PYTHON_VERSION} -D__EMSCRIPTEN__=1"
export CXXFLAGS="--target=${TARGET} -fPIC -I${CROSS_BUILD}/install/include/python${PYTHON_VERSION}"
export LDSHARED=${CC}
export AR="${WASI_SDK_PATH}/bin/ar"
export RANLIB=true
export LDFLAGS="--target=${TARGET} -shared -Wl,--allow-undefined"
export _PYTHON_SYSCONFIGDATA_NAME=_sysconfigdata__wasi_wasm32-wasi
export _PYTHON_HOST_PLATFORM=wasm32-wasi

python3 -m build -n -w
wheel unpack --dest build dist/*.whl

rm -rf ./wasm_venv
