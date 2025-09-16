#!/usr/bin/env python3
"""
Script to create GitHub releases for SingleStoreDB Python SDK.

This script automatically:
1. Extracts the current version from setup.cfg
2. Finds the matching release notes from docs/src/whatsnew.rst
3. Creates a temporary notes file
4. Creates a GitHub release using gh CLI
5. Cleans up temporary files

Usage:
    python create_release.py [--version VERSION] [--dry-run]

Examples:
    python create_release.py                    # Use current version from setup.cfg
    python create_release.py --version 1.15.6   # Use specific version
    python create_release.py --dry-run          # Preview without executing
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path


def get_version_from_setup_cfg() -> str:
    """Extract the current version from setup.cfg."""
    setup_cfg_path = Path(__file__).parent.parent / 'setup.cfg'

    if not setup_cfg_path.exists():
        raise FileNotFoundError(f'Could not find setup.cfg at {setup_cfg_path}')

    with open(setup_cfg_path, 'r') as f:
        content = f.read()

    match = re.search(r'^version\s*=\s*(.+)$', content, re.MULTILINE)
    if not match:
        raise ValueError('Could not find version in setup.cfg')

    return match.group(1).strip()


def extract_release_notes(version: str) -> str:
    """Extract release notes for the specified version from whatsnew.rst."""
    whatsnew_path = Path(__file__).parent.parent / 'docs' / 'src' / 'whatsnew.rst'

    if not whatsnew_path.exists():
        raise FileNotFoundError(f'Could not find whatsnew.rst at {whatsnew_path}')

    with open(whatsnew_path, 'r') as f:
        content = f.read()

    # Look for the version section
    version_pattern = rf'^v{re.escape(version)}\s*-\s*.*$'
    version_match = re.search(version_pattern, content, re.MULTILINE)

    if not version_match:
        raise ValueError(f'Could not find release notes for version {version} in whatsnew.rst')

    # Find the start of the version section
    start_pos = version_match.end()

    # Find the separator line (dashes)
    lines = content[start_pos:].split('\n')
    notes_start = None

    for i, line in enumerate(lines):
        if line.strip() and all(c == '-' for c in line.strip()):
            notes_start = i + 1
            break

    if notes_start is None:
        raise ValueError(f'Could not find release notes separator for version {version}')

    # Extract notes until the next version section
    notes_lines: list[str] = []
    for line in lines[notes_start:]:
        # Stop at the next version (starts with 'v' followed by a number)
        if re.match(r'^v\d+\.\d+\.\d+', line.strip()):
            break
        # Stop at empty line followed by version line
        if line.strip() == '' and notes_lines:
            # Check if next non-empty line is a version
            remaining_lines = lines[notes_start + len(notes_lines) + 1:]
            for next_line in remaining_lines:
                if next_line.strip():
                    if re.match(r'^v\d+\.\d+\.\d+', next_line.strip()):
                        break
                    break
            else:
                continue
        notes_lines.append(line)

    # Clean up the notes
    # Remove trailing empty lines
    while notes_lines and not notes_lines[-1].strip():
        notes_lines.pop()

    if not notes_lines:
        raise ValueError(f'No release notes found for version {version}')

    return '\n'.join(notes_lines)


def create_release(version: str, notes: str, dry_run: bool = False) -> None:
    """Create a GitHub release using gh CLI."""
    # Create temporary file for release notes
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        f.write(notes)
        notes_file = f.name

    try:
        # Construct the gh release create command
        tag = f'v{version}'
        title = f'SingleStoreDB v{version}'

        cmd = [
            'gh', 'release', 'create', tag,
            '--title', title,
            '--notes-file', notes_file,
        ]

        if dry_run:
            print('DRY RUN: Would execute the following command:')
            print(' '.join(cmd))
            print(f'\nRelease notes file content:')
            print(f"{'='*50}")
            print(notes)
            print(f"{'='*50}")
            return

        print(f'Creating GitHub release for version {version}...')
        print(f'Tag: {tag}')
        print(f'Title: {title}')
        print(f'Notes file: {notes_file}')

        # Execute the command
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            print('Error creating GitHub release:')
            print(f'STDOUT: {result.stdout}')
            print(f'STDERR: {result.stderr}')
            sys.exit(1)

        print('✅ GitHub release created successfully!')
        print(f'Output: {result.stdout}')

    finally:
        # Clean up temporary file
        try:
            os.unlink(notes_file)
        except OSError:
            pass


def check_prerequisites() -> None:
    """Check that required tools are available."""
    # Check if gh CLI is available
    try:
        result = subprocess.run(['gh', '--version'], capture_output=True, text=True, check=True)
        print(f'GitHub CLI found: {result.stdout.strip().split()[2]}')
    except (subprocess.CalledProcessError, FileNotFoundError):
        print('Error: GitHub CLI (gh) is not installed or not in PATH')
        print('Please install it from https://cli.github.com/')
        sys.exit(1)

    # Check if we're in a git repository
    try:
        subprocess.run(['git', 'rev-parse', '--git-dir'], capture_output=True, check=True)
    except subprocess.CalledProcessError:
        print('Error: Not in a git repository')
        sys.exit(1)

    # Check if we're authenticated with GitHub
    try:
        result = subprocess.run(['gh', 'auth', 'status'], capture_output=True, text=True)
        if result.returncode != 0:
            print('Error: Not authenticated with GitHub')
            print('Please run: gh auth login')
            sys.exit(1)
    except subprocess.CalledProcessError:
        print('Error: Could not check GitHub authentication status')
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Create GitHub release for SingleStoreDB Python SDK',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''Examples:
  %(prog)s                        # Use current version from setup.cfg
  %(prog)s --version 1.15.6       # Use specific version
  %(prog)s --dry-run               # Preview without executing''',
    )

    parser.add_argument(
        '--version',
        help='Version to release (default: extract from setup.cfg)',
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without executing',
    )

    args = parser.parse_args()

    try:
        # Check prerequisites
        if not args.dry_run:
            check_prerequisites()

        # Get version
        if args.version:
            version = args.version
            print(f'Using specified version: {version}')
        else:
            version = get_version_from_setup_cfg()
            print(f'Extracted version from setup.cfg: {version}')

        # Extract release notes
        print(f'Extracting release notes for version {version}...')
        notes = extract_release_notes(version)

        # Create the release
        create_release(version, notes, dry_run=args.dry_run)

        if not args.dry_run:
            print(f'\n✅ Successfully created GitHub release for v{version}')

    except Exception as e:
        print(f'Error: {e}')
        sys.exit(1)


if __name__ == '__main__':
    main()
