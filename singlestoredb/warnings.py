#!/usr/bin/env python

class PreviewFeatureWarning(UserWarning):
    """Warning for experimental preview features."""
    pass


class DeprecatedFeatureWarning(UserWarning):
    """
    Warning for deprecated features that still work.

    Deliberately a ``UserWarning`` rather than a ``DeprecationWarning``, for the
    same reason :class:`PreviewFeatureWarning` is: Python ignores
    ``DeprecationWarning`` by default outside ``__main__``, and these fire from
    library frames several calls below the notebook cell that triggered them, so
    a ``DeprecationWarning`` would reach almost nobody. The Python-level
    management API uses the builtin there (see
    :func:`singlestoredb.manage_workspaces`), where the caller's own frame is
    close enough for the default filter to do the right thing.
    """
    pass
