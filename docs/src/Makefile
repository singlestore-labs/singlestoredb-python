# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = .
BUILDDIR      = _build

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@# Start SingleStoreDB if SINGLESTOREDB_URL is not already set
	@if [ -z "$$SINGLESTOREDB_URL" ]; then \
		echo "Starting SingleStoreDB Docker container..."; \
		export SINGLESTOREDB_URL=$$(python3 ./singlestoredb-docker.py start); \
		if [ $$? -ne 0 ]; then exit 1; fi; \
	fi; \
	rm -rf "$(BUILDDIR)/html" && \
	$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O) && \
	./post-process-html "$(BUILDDIR)/html" && \
	rm -rf ../_images && \
	rm -rf ../_sources && \
	rm -rf ../_static && \
	rm -rf ../generated && \
	mv "$(BUILDDIR)"/html/* .. && \
	mv "$(BUILDDIR)"/html/.[!.]* .. || true
	@# Stop the container if we started it
	@if [ -z "$$SINGLESTOREDB_URL" ] && [ -f .singlestore_docs_container.txt ]; then \
		echo "Stopping SingleStoreDB Docker container..."; \
		python3 ./singlestoredb-docker.py stop; \
	fi
