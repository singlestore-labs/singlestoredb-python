import os
import tempfile
from pathlib import Path
from typing import Any
from warnings import warn

from IPython.core.interactiveshell import InteractiveShell
from IPython.core.magic import line_magic
from IPython.core.magic import Magics
from IPython.core.magic import magics_class
from IPython.core.magic import needs_local_scope
from IPython.core.magic import no_var_expand
from IPython.utils.contexts import preserve_keys
from IPython.utils.syspathcontext import prepended_to_syspath
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
            with preserve_keys(self.shell.user_ns, '__file__'):
                self.shell.user_ns['__file__'] = temp_file_path
                self.safe_execfile_ipy(temp_file_path, raise_exceptions=True)

    def safe_execfile_ipy(
        self,
        fname: str,
        shell_futures: bool = False,
        raise_exceptions: bool = False,
    ) -> None:
        """Like safe_execfile, but for .ipy or .ipynb files with IPython syntax.

        Parameters
        ----------
        fname : str
            The name of the file to execute.  The filename must have a
            .ipy or .ipynb extension.
        shell_futures : bool (False)
            If True, the code will share future statements with the interactive
            shell. It will both be affected by previous __future__ imports, and
            any __future__ imports in the code will affect the shell. If False,
            __future__ imports are not shared in either direction.
        raise_exceptions : bool (False)
            If True raise exceptions everywhere.  Meant for testing.
        """
        fpath = Path(fname).expanduser().resolve()

        # Make sure we can open the file
        try:
            with fpath.open('rb'):
                pass
        except Exception:
            warn('Could not open file <%s> for safe execution.' % fpath)
            return

        # Find things also in current directory.  This is needed to mimic the
        # behavior of running a script from the system command line, where
        # Python inserts the script's directory into sys.path
        dname = str(fpath.parent)

        def get_cells() -> Any:
            """generator for sequence of code blocks to run"""
            if fpath.suffix == '.ipynb':
                from nbformat import read
                nb = read(fpath, as_version=4)
                if not nb.cells:
                    return
                for cell in nb.cells:
                    if cell.cell_type == 'code':
                        if not cell.source.strip():
                            continue
                        if getattr(cell, 'metadata', {}).get('language', '') == 'sql':
                            output_redirect = getattr(
                                cell, 'metadata', {},
                            ).get('output_variable', '') or ''
                            if output_redirect:
                                output_redirect = f' {output_redirect} <<'
                            yield f'%%sql{output_redirect}\n{cell.source}'
                        else:
                            yield cell.source
            else:
                yield fpath.read_text(encoding='utf-8')

        with prepended_to_syspath(dname):
            try:
                for cell in get_cells():
                    result = self.shell.run_cell(
                        cell, silent=True, shell_futures=shell_futures,
                    )
                    if raise_exceptions:
                        result.raise_error()
                    elif not result.success:
                        break
            except Exception:
                if raise_exceptions:
                    raise
                self.shell.showtraceback()
                warn('Unknown failure executing file: <%s>' % fpath)
