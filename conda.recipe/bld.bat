@echo on

dir
dir dist

"%PYTHON%" -m pip install --no-deps --ignore-installed dist/singlestoredb-0.4.0-cp36-abi3-win_amd64.whl
if errorlevel 1 exit 1
