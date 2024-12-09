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
class RunSharedMagic(Magics):
    def __init__(self, shell: InteractiveShell):
        Magics.__init__(self, shell=shell)

    @no_var_expand
    @needs_local_scope
    @line_magic('run_shared')
    def run_shared(self, line: str, local_ns: Any = None) -> Any:
        """
        Downloads a shared file using the %sql magic and then runs it using %run.

        Examples::

          # Line usage

          %run_shared shared_file.ipynb

          %run_shared {{ sample_notebook_name }}

        """
        template = Template(line.strip())
        shared_file = template.render(local_ns)
        if not shared_file:
            raise ValueError('No shared file specified.')
        if (shared_file.startswith("'") and shared_file.endswith("'")) or \
           (shared_file.startswith('"') and shared_file.endswith('"')):
            shared_file = shared_file[1:-1]
            if not shared_file:
                raise ValueError('No personal file specified.')

        local_filename = f'{int(time.time() * 1_000_000)}_{shared_file}'.replace(' ', '_')
        sql_command = f"DOWNLOAD SHARED FILE '{shared_file}' TO '{local_filename}'"

        # Execute the SQL command
        self.shell.run_line_magic('sql', sql_command)
        # Run the downloaded file
        self.shell.run_line_magic('run', local_filename)

        # Delete the local file after running it
        if os.path.exists(local_filename):
            os.remove(local_filename)
