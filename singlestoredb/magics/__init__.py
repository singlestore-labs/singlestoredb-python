from IPython.core.interactiveshell import InteractiveShell

from .run_personal import RunPersonalMagic
from .run_shared import RunSharedMagic

# In order to actually use these magics, we must register them with a
# running IPython.


def load_ipython_extension(ip: InteractiveShell) -> None:
    """
    Any module file that define a function named `load_ipython_extension`
    can be loaded via `%load_ext module.path` or be configured to be
    autoloaded by IPython at startup time.
    """

    # Load jupysql extension
    # This is necessary for jupysql to initialize internal state
    # required to render messages
    assert ip.extension_manager is not None
    result = ip.extension_manager.load_extension('sql')
    if result == 'no load function':
        raise RuntimeError('Could not load sql extension. Is jupysql installed?')

    # Check if %run magic command is defined
    if ip.find_line_magic('run') is None:
        raise RuntimeError(
            '%run magic command is not defined. '
            'Is it available in your IPython environment?',
        )

    # Register run_personal and run_shared
    ip.register_magics(RunPersonalMagic(ip))
    ip.register_magics(RunSharedMagic(ip))
