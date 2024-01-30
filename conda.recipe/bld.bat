@echo on

dir
dir dist

"%PYTHON%" -m pip install --no-deps --ignore-installed "dist/singlestoredb-%PKG_VERSION%-cp36-abi3-win_amd64.whl"
if errorlevel 1 exit 1
