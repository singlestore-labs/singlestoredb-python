@echo on

dir

python -m pip install --no-deps --ignore-installed %SRC_DIR\%dist\singlestoredb-*win*.whl
