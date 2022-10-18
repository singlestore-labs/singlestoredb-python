# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
# -- Path setup --------------------------------------------------------------
# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
sys.path.insert(0, os.path.abspath('../..'))


# -- Project information -----------------------------------------------------

project = 'SingleStoreDB'
copyright = '2022 SingleStore. All Rights Reserved'
author = 'SingleStore'


import singlestoredb as s2  # noqa: W291,E402

version = s2.__version__
release = s2.__version__


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autosummary',
    'sphinx.ext.napoleon',
    'sphinx.ext.githubpages',
    'IPython.sphinxext.ipython_directive',
    'IPython.sphinxext.ipython_console_highlighting',
    'sphinx.ext.intersphinx',
]

autosummary_generate = True
numpydoc_show_class_members = False
autodoc_default_flags = ['show-inheritance']
autoclass_content = 'class'

intersphinx_mapping = {
    'python': ('https://docs.python.org/', None),
    'pandas': ('http://pandas.pydata.org/pandas-docs/version/0.19.2/', None),
    'numpy': ('http://docs.scipy.org/doc/numpy/', None),
    'scipy': ('http://docs.scipy.org/doc/scipy/reference/', None),
    'matplotlib': ('http://matplotlib.sourceforge.net/', None),
}

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']


# -- Options for HTML output -------------------------------------------------

import sphinx_rtd_theme  # noqa: E402

html_theme = 'sphinx_rtd_theme'

html_context = {
    'css_files': [
        '_static/pygments.css',
        '_static/css/theme.css',
        '_static/custom.css',
    ],
}

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
# html_theme_options = {}

# Add any paths that contain custom themes here, relative to this directory.
html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

pygments_style = 'monokai'
#pygments_dark_style = 'monokai'
