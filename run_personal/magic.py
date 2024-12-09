import os
import time
from typing import Any

from IPython.core.interactiveshell import InteractiveShell
from IPython.core.magic import line_magic
from IPython.core.magic import Magics
from IPython.core.magic import magics_class
from IPython.core.magic import needs_local_scope
from IPython.core.magic import no_var_expand
from jinja2 import Template


@magics_class
class RunPersonalMagic(Magics):
    def __init__(self, shell: InteractiveShell):
        Magics.__init__(self, shell=shell)

    @no_var_expand
    @needs_local_scope
    @line_magic('run_personal')
    def run_personal(self, line: str, local_ns: Any = None) -> Any:
        """
        Downloads a personal file using the %sql magic and then runs it using %run.

        Examples::

          # Line usage

          %run_personal personal_file.ipynb

          %run_personal {{ sample_notebook_name }}

        """
        template = Template(line.strip())
        personal_file = template.render(local_ns)
        if not personal_file:
            raise ValueError('No personal file specified.')
        if (personal_file.startswith("'") and personal_file.endswith("'")) or \
           (personal_file.startswith('"') and personal_file.endswith('"')):
            personal_file = personal_file[1:-1]
            if not personal_file:
                raise ValueError('No personal file specified.')

        local_filename = (
            f'{int(time.time() * 1_000_000)}_{personal_file}'.replace(' ', '_')
        )
        sql_command = f"DOWNLOAD PERSONAL FILE '{personal_file}' TO '{local_filename}'"

        # Execute the SQL command
        self.shell.run_line_magic('sql', sql_command)
        # Run the downloaded file
        self.shell.run_line_magic('run', local_filename)

        # Delete the local file after running it
        if os.path.exists(local_filename):
            os.remove(local_filename)


# In order to actually use these magics, you must register them with a
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

    # Register run_personal
    ip.register_magics(RunPersonalMagic(ip))
