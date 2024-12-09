import os
import tempfile
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

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file_path = os.path.join(temp_dir, personal_file)
            sql_command = (
                f"DOWNLOAD PERSONAL FILE '{personal_file}' "
                f"TO '{temp_file_path}'"
            )

            # Execute the SQL command
            self.shell.run_line_magic('sql', sql_command)
            # Run the downloaded file
            self.shell.run_line_magic('run', f'"{temp_file_path}"')
