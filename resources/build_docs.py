#!/usr/bin/env python3
"""
Unified documentation build script for SingleStoreDB Python SDK.

Usage:
    python build_docs.py [target] [options]

    Can be run from the repository root or resources directory.
    The script automatically detects its location and works with the docs/src directory.

Examples:
    python resources/build_docs.py html              # From repo root
    python build_docs.py html                        # From resources directory
    python build_docs.py help                        # Show Sphinx help
    python build_docs.py html --verbose              # Build with verbose output
    python build_docs.py html --no-docker            # Build without starting Docker

"""
from __future__ import annotations

import argparse
import glob
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from re import Match
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

# Detect script location and set up paths
def get_repo_paths() -> tuple[Path, Path]:
    """Determine the repository root and docs source directory based on script location."""
    script_dir = Path(__file__).parent.absolute()

    # Check if we're in the resources directory or repo root
    if script_dir.name == 'resources':
        repo_root = script_dir.parent
        docs_src = repo_root / 'docs' / 'src'
    elif (script_dir / 'singlestoredb').exists():
        # We're in the repo root
        repo_root = script_dir
        docs_src = repo_root / 'docs' / 'src'
    else:
        # Try to find the repo root by looking for singlestoredb directory
        current = script_dir
        while current != current.parent:
            if (current / 'singlestoredb').exists():
                repo_root = current
                docs_src = repo_root / 'docs' / 'src'
                break
            current = current.parent
        else:
            raise RuntimeError(f'Could not find repository root from {script_dir}')

    return repo_root, docs_src

REPO_ROOT, DOCS_SRC = get_repo_paths()

# Add the repo root to the path so we can import singlestoredb
sys.path.insert(0, str(REPO_ROOT))

from singlestoredb.server import docker

# Constants using absolute paths
CONTAINER_INFO_FILE = DOCS_SRC / '.singlestore_docs_container.txt'
BUILD_DIR = DOCS_SRC / '_build'
SOURCE_DIR = DOCS_SRC
CUSTOM_CSS_FILE = DOCS_SRC / 'custom.css'


class DocBuilder:
    """Main documentation builder class."""

    def __init__(
        self, target: str = 'html', verbose: bool = False,
        use_docker: bool = True, sphinx_opts: str = '',
    ):
        self.target = target
        self.verbose = verbose
        self.use_docker = use_docker
        self.sphinx_opts = sphinx_opts
        self.container_started = False
        self.container_url: Optional[str] = None

        # Store paths for operations
        self.docs_src = DOCS_SRC
        self.build_dir = BUILD_DIR
        self.source_dir = SOURCE_DIR
        self.custom_css_file = CUSTOM_CSS_FILE
        self.container_info_file = CONTAINER_INFO_FILE

        # Ensure docs/src directory exists
        if not self.docs_src.exists():
            raise RuntimeError(f'Documentation source directory not found: {self.docs_src}')

    def log(self, message: str, error: bool = False) -> None:
        """Log a message to stderr if verbose mode is enabled."""
        if self.verbose or error:
            print(message, file=sys.stderr)

    def status(self, message: str) -> None:
        """Always show status messages to indicate progress."""
        print(f'📋 {message}', file=sys.stderr)

    def step(self, step_num: int, total_steps: int, message: str) -> None:
        """Show a numbered step with progress."""
        print(f'📍 Step {step_num}/{total_steps}: {message}', file=sys.stderr)

    # Docker Management Functions
    def start_docker_container(self) -> bool:
        """Start SingleStoreDB Docker container if needed."""
        # Check if SINGLESTOREDB_URL is already set
        if os.environ.get('SINGLESTOREDB_URL'):
            self.status(f"Using existing database: {os.environ['SINGLESTOREDB_URL']}")
            return True

        if not self.use_docker:
            self.status('Docker disabled, skipping container start')
            return True

        self.status('🐳 Starting SingleStoreDB Docker container...')
        start_time = time.time()

        try:
            # Start the container
            server = docker.start()
            self.container_url = server.connection_url

            # Save the container info for stopping later
            with open(self.container_info_file, 'w') as f:
                f.write(f'{server.container.name}\n')
                f.write(f'{self.container_url}\n')

            # Set environment variable
            os.environ['SINGLESTOREDB_URL'] = self.container_url
            self.container_started = True

            # Wait for the container to be ready
            if not self.wait_for_container_ready(server):
                return False

            elapsed = time.time() - start_time
            self.status(f'✅ Database ready in {elapsed:.1f}s: {self.container_url}')
            return True

        except Exception as e:
            self.log(f'Error starting Docker container: {e}', error=True)
            return False

    def wait_for_container_ready(self, server: Any, max_retries: int = 30) -> bool:
        """Wait for the Docker container to be ready."""
        print('   ⏳ Waiting for database to be ready...', end='', flush=True, file=sys.stderr)

        for attempt in range(max_retries):
            try:
                test_conn = server.connect()
                test_conn.close()
                print('', file=sys.stderr)  # New line after dots
                return True
            except Exception as e:
                if attempt == max_retries - 1:
                    print('', file=sys.stderr)  # New line after dots
                    self.log(f'❌ Container failed to start after {max_retries} seconds', error=True)
                    self.log(f'Last error: {e}', error=True)
                    return False
                print('.', end='', flush=True, file=sys.stderr)
                time.sleep(1)
        return False

    def stop_docker_container(self) -> bool:
        """Stop and remove the SingleStoreDB Docker container."""
        if not self.container_started or not self.container_info_file.exists():
            return True

        try:
            with open(self.container_info_file, 'r') as f:
                lines = f.readlines()
                if not lines:
                    return True
                container_name = lines[0].strip()

            self.status(f'🛑 Stopping container: {container_name}')

            # Stop the container
            subprocess.run(
                ['docker', 'stop', container_name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

            # Remove the container
            subprocess.run(
                ['docker', 'rm', container_name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

            # Remove the info file
            self.container_info_file.unlink()
            self.log('SingleStoreDB container stopped and removed.')
            return True

        except Exception as e:
            self.log(f'Error stopping container: {e}', error=True)
            # Still try to remove the info file
            if self.container_info_file.exists():
                self.container_info_file.unlink()
            return False

    # Build Functions
    def clean_build_directory(self) -> None:
        """Clean the build directory."""
        html_build_dir = self.build_dir / 'html'
        if html_build_dir.exists():
            self.log(f'Cleaning build directory: {html_build_dir}')
            shutil.rmtree(html_build_dir)

    def copy_custom_css(self) -> bool:
        """Copy custom CSS to the _static directory."""
        try:
            static_dir = self.build_dir / 'html' / '_static'
            static_dir.mkdir(parents=True, exist_ok=True)

            custom_css_dest = static_dir / 'custom.css'
            with open(custom_css_dest, 'w') as dest_file:
                with open(self.custom_css_file, 'r') as src_file:
                    dest_file.write(src_file.read().strip())

            self.log(f'Copied {self.custom_css_file} to {custom_css_dest}')
            return True

        except Exception as e:
            self.log(f'Error copying custom CSS: {e}', error=True)
            return False

    def run_sphinx_build(self) -> bool:
        """Run sphinx-build with the specified target."""
        if self.target == 'help':
            # Show Sphinx help
            cmd = ['sphinx-build', '-M', 'help', str(self.source_dir), str(self.build_dir)]
            try:
                subprocess.run(cmd, check=True)
                return True
            except subprocess.CalledProcessError as e:
                self.log(f'Error running Sphinx help: {e}', error=True)
                return False

        # Clean build directory first
        self.status('🧹 Cleaning build directory...')
        self.clean_build_directory()

        # Build the command
        cmd = ['sphinx-build', '-M', self.target, str(self.source_dir), str(self.build_dir)]

        # Add any additional Sphinx options
        if self.sphinx_opts:
            cmd.extend(self.sphinx_opts.split())

        self.status(f'🔨 Building {self.target} documentation...')
        self.log(f'Command: {" ".join(cmd)}')

        start_time = time.time()

        try:
            # Run sphinx-build - always show output for better feedback
            result = subprocess.run(cmd, check=True)

            elapsed = time.time() - start_time
            self.status(f'✅ Sphinx build completed in {elapsed:.1f}s')
            return True

        except subprocess.CalledProcessError as e:
            elapsed = time.time() - start_time
            self.log(f'❌ Sphinx build failed after {elapsed:.1f}s (exit code: {e.returncode})', error=True)
            return False

    # HTML Post-processing Functions
    def collect_generated_links(self, build_html_dir: Path) -> Dict[str, str]:
        """Collect links from generated HTML files."""
        links: Dict[str, str] = {}
        generated_dir = build_html_dir / 'generated'

        if not generated_dir.exists():
            return links

        for html_file in generated_dir.glob('*.html'):
            # Match class names like "ClassName.html"
            m = re.search(r'([A-Z]\w+)\.html$', html_file.name)
            if m:
                links[m.group(1)] = html_file.name
                continue

            # Match method names like "ClassName.method_name.html"
            m = re.search(r'([A-Z]\w+\.[a-z]\w+)\.html$', html_file.name)
            if m:
                links[m.group(1)] = html_file.name

        if links:
            self.log(f'Found {len(links)} generated API reference links')
        return links

    def check_link(self, match: Match[str], links: Dict[str, str]) -> str:
        """Check and fix links in HTML content."""
        link, pre, txt, post = match.groups()
        if not link and txt in links:
            return f'<a href="{links[txt]}">{pre}{txt}{post}</a>'
        return match.group(0)

    def process_html_files(self, build_html_dir: Path) -> bool:
        """Post-process HTML files with various transformations."""
        try:
            self.status('🔗 Collecting generated API links...')
            start_time = time.time()

            # Collect generated links
            links = self.collect_generated_links(build_html_dir)

            # Copy custom CSS to _static directory
            self.status('🎨 Copying custom CSS...')
            if not self.copy_custom_css():
                return False

            # Get list of files to process
            self.status('🔍 Scanning files for post-processing...')
            files_to_process: List[Path] = []
            for ext in ['html', 'txt', 'svg', 'js', 'css', 'rst']:
                files_to_process.extend(build_html_dir.rglob(f'*.{ext}'))

            total_files = len(files_to_process)
            self.status(f'✏️  Processing {total_files} files...')

            # Process each file with progress indicator
            for i, file_path in enumerate(sorted(files_to_process), 1):
                try:
                    if i % 50 == 0 or i == total_files:  # Show progress every 50 files or at end
                        print(f'   📄 {i}/{total_files} files processed', file=sys.stderr)

                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()

                    # Apply transformations
                    content = self.apply_content_transformations(content, links)

                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(content)

                except Exception as e:
                    self.log(f'Error processing file {file_path}: {e}', error=True)
                    # Continue processing other files

            elapsed = time.time() - start_time
            self.status(f'✅ Post-processing completed in {elapsed:.1f}s')
            return True

        except Exception as e:
            self.log(f'❌ Error in HTML post-processing: {e}', error=True)
            return False

    def apply_content_transformations(self, content: str, links: Dict[str, str]) -> str:
        """Apply all content transformations to a text file."""
        # Remove module names from hidden modules
        content = re.sub(
            r'(">)singlestoredb\.(connection|management)\.([\w\.]+</a>)',
            r'\1\3',
            content,
        )

        # Remove singleton representations
        content = re.sub(
            r'Organization\(name=.+?\)',
            r'<em class="property"><span class="w"> </span><span class="p"></span><span class="w"> </span><span class="pre">&lt;singlestoredb.notebook._objects.Organization</span> <span class="pre">object&gt;</span></em>',
            content,
        )

        # Change ShowAccessor to Connection.show
        content = re.sub(r'>ShowAccessor\.', r'>Connection.show.', content)

        # Change workspace.Stage to workspace.stage
        content = re.sub(r'>workspace\.Stage\.', r'>workspace.stage.', content)

        # Fix class/method links
        content = re.sub(
            r'(<a\s+[^>]+>)?(\s*<code[^>]*>\s*<span\s+class="pre">\s*)([\w\.]+)(\s*</span>\s*</code>)',
            lambda m: self.check_link(m, links),
            content,
        )

        # Trim trailing whitespace
        content = re.sub(r'\s+\n', r'\n', content)

        # Fix end-of-files
        content = re.sub(r'\s*$', r'', content) + '\n'

        return content

    # File Management Functions
    def cleanup_old_files(self) -> None:
        """Remove old documentation files from parent directory."""
        parent_dir = self.docs_src.parent  # docs directory
        dirs_to_remove = ['_images', '_sources', '_static', 'generated']

        for dir_name in dirs_to_remove:
            dir_path = parent_dir / dir_name
            if dir_path.exists():
                self.log(f'Removing old directory: {dir_path}')
                shutil.rmtree(dir_path)

    def move_generated_files(self, build_html_dir: Path) -> bool:
        """Move generated files to parent directory."""
        try:
            self.status('📁 Moving documentation files to docs/ directory...')
            start_time = time.time()

            parent_dir = self.docs_src.parent  # docs directory

            # First cleanup old files
            self.cleanup_old_files()

            # Count items to move
            items = list(build_html_dir.iterdir())
            total_items = len(items)

            # Move all files from build HTML directory to parent
            for i, item in enumerate(items, 1):
                dest_path = parent_dir / item.name

                if item.is_dir():
                    if dest_path.exists():
                        shutil.rmtree(dest_path)
                    shutil.copytree(item, dest_path)
                    self.log(f'Moved directory: {item.name}')
                else:
                    shutil.copy2(item, dest_path)
                    self.log(f'Moved file: {item.name}')

                # Show progress
                if i % 10 == 0 or i == total_items:
                    print(f'   📦 {i}/{total_items} items moved', file=sys.stderr)

            elapsed = time.time() - start_time
            self.status(f'✅ File movement completed in {elapsed:.1f}s')
            return True

        except Exception as e:
            self.log(f'❌ Error moving generated files: {e}', error=True)
            return False

    # Main Orchestration
    def build_docs(self) -> int:
        """Main function to build documentation."""
        try:
            total_start_time = time.time()

            # Determine total steps
            total_steps = 4 if self.target == 'html' else 2

            print(f'🚀 Starting SingleStoreDB documentation build ({self.target})', file=sys.stderr)
            print(f'📍 Working directory: {self.docs_src}', file=sys.stderr)
            print('=' * 60, file=sys.stderr)

            # Step 1: Start Docker container if needed
            self.step(1, total_steps, 'Database setup')
            if not self.start_docker_container():
                return 1

            # Step 2: Run Sphinx build
            self.step(2, total_steps, f'Sphinx {self.target} build')
            if not self.run_sphinx_build():
                return 1

            # For HTML builds, do post-processing and file movement
            if self.target == 'html':
                build_html_dir = self.build_dir / 'html'

                # Step 3: Post-process HTML files
                self.step(3, total_steps, 'HTML post-processing')
                if not self.process_html_files(build_html_dir):
                    return 1

                # Step 4: Move generated files to parent directory
                self.step(4, total_steps, 'File deployment')
                if not self.move_generated_files(build_html_dir):
                    return 1

            total_elapsed = time.time() - total_start_time
            print('=' * 60, file=sys.stderr)
            print(f'🎉 Documentation build completed successfully in {total_elapsed:.1f}s!', file=sys.stderr)
            return 0

        except KeyboardInterrupt:
            print('\n❌ Build interrupted by user', file=sys.stderr)
            return 1
        except Exception as e:
            print(f'\n❌ Unexpected error during build: {e}', file=sys.stderr)
            return 1
        finally:
            # Always try to stop the container if we started it
            if self.container_started:
                self.stop_docker_container()


def main() -> int:
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Build SingleStoreDB Python SDK documentation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s html                 Build HTML documentation
  %(prog)s help                 Show Sphinx help
  %(prog)s html --verbose       Build with verbose output
  %(prog)s html --no-docker     Build without starting Docker
  %(prog)s latexpdf             Build PDF documentation
  %(prog)s html --sphinx-opts "-W --keep-going"  Pass options to Sphinx
        """.strip(),
    )

    parser.add_argument(
        'target',
        nargs='?',
        default='html',
        help='Sphinx build target (default: html)',
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose output',
    )

    parser.add_argument(
        '--no-docker',
        action='store_true',
        help='Do not start/stop Docker container',
    )

    parser.add_argument(
        '--sphinx-opts',
        default='',
        help='Additional options to pass to sphinx-build',
    )

    args = parser.parse_args()

    # Create builder instance
    builder = DocBuilder(
        target=args.target,
        verbose=args.verbose,
        use_docker=not args.no_docker,
        sphinx_opts=args.sphinx_opts,
    )

    # Build documentation
    return builder.build_docs()


if __name__ == '__main__':
    sys.exit(main())
