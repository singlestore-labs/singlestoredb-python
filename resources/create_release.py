#!/usr/bin/env python3
"""
Script to create GitHub releases for SingleStoreDB Python SDK.

This script automatically:
1. Fetches origin/main and extracts its version from pyproject.toml
2. Finds the commit on origin/main that merged the release pull request
3. Reads the matching release notes from that commit's docs/src/whatsnew.rst
   into a temporary notes file
4. Creates a GitHub release using gh CLI, tagging that commit
5. Cleans up temporary files

Everything is read from origin/main rather than the working tree, because the
version bump reaches main through a pull request. Releasing before that PR is
merged would otherwise tag the previous release's code with the new version.
The tag goes on the release commit rather than the tip, so PRs merged after
the release are not published with it.

Usage:
    python create_release.py [--version VERSION] [--dry-run]

Examples:
    python create_release.py                    # Use the version on origin/main
    python create_release.py --version 1.15.6   # Check origin/main is at this version
    python create_release.py --dry-run          # Preview without executing
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tempfile
import time


def status(message: str) -> None:
    """Show status messages to indicate progress."""
    print(f'📋 {message}', file=sys.stderr)


def step(step_num: int, total_steps: int, message: str) -> None:
    """Show a numbered step with progress."""
    print(f'📍 Step {step_num}/{total_steps}: {message}', file=sys.stderr)


RELEASE_REF = 'origin/main'


def git(*args: str) -> str:
    """Run a git command and return its stdout."""
    result = subprocess.run(['git', *args], capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f'git {" ".join(args)} failed: {result.stderr.strip()}')
    return result.stdout


def fetch_release_ref() -> str:
    """Fetch origin/main and return the commit at its tip."""
    git('fetch', 'origin', 'main')
    return git('rev-parse', RELEASE_REF).strip()


def find_release_commit(version: str) -> str:
    """Return the commit on origin/main that merged the release PR.

    That is the commit to tag, not the tip: anything merged after it is not
    in this release. The release PR is squash-merged, so its commit's subject
    is the PR title, with the PR number GitHub appends.
    """
    pattern = rf'^Prepare for v{re.escape(version)} release( \(#[0-9]+\))?$'
    commit = git(
        'log', RELEASE_REF, '-1', '--format=%H',
        '--extended-regexp', f'--grep={pattern}',
    ).strip()
    if not commit:
        raise ValueError(
            f'No "Prepare for v{version} release" commit on {RELEASE_REF}; '
            'has the release pull request been merged?',
        )
    return commit


def read_file_at(commit: str, path: str) -> str:
    """Read a file's contents as of the given commit."""
    return git('show', f'{commit}:{path}')


def check_tag_absent(tag: str) -> None:
    """Make sure the release tag does not already exist on origin."""
    if git('ls-remote', '--tags', 'origin', f'refs/tags/{tag}').strip():
        raise ValueError(
            f'Tag {tag} already exists on origin; if this release is new, '
            'its pull request has not been merged yet',
        )


def get_version_from_pyproject(commit: str) -> str:
    """Extract the version from pyproject.toml as of the given commit."""
    content = read_file_at(commit, 'pyproject.toml')

    match = re.search(r'^version\s*=\s*["\'](.+)["\']$', content, re.MULTILINE)
    if not match:
        raise ValueError('Could not find version in pyproject.toml')

    return match.group(1).strip()


def extract_release_notes(version: str, commit: str) -> str:
    """Extract release notes for the specified version from whatsnew.rst."""
    content = read_file_at(commit, 'docs/src/whatsnew.rst')

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


def create_release(version: str, notes: str, commit: str, dry_run: bool = False) -> None:
    """Create a GitHub release using gh CLI."""
    tag = f'v{version}'
    title = f'SingleStoreDB v{version}'

    # Create temporary file for release notes
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        f.write(notes)
        notes_file = f.name

    try:
        # Construct the gh release create command. --target pins the new tag
        # to the commit the version and notes were read from; without it, gh
        # tags whatever main is when the API call lands.
        cmd = [
            'gh', 'release', 'create', tag,
            '--target', commit,
            '--title', title,
            '--notes-file', notes_file,
        ]

        if dry_run:
            status('🔍 DRY RUN - Preview mode')
            print('=' * 50, file=sys.stderr)
            print(f'Command: {" ".join(cmd)}', file=sys.stderr)
            print(f'Tag: {tag}', file=sys.stderr)
            print(f'Target: {commit}', file=sys.stderr)
            print(f'Title: {title}', file=sys.stderr)
            print(f'Notes file: {notes_file}', file=sys.stderr)
            print('=' * 50, file=sys.stderr)
            print('Release notes content:', file=sys.stderr)
            print(notes, file=sys.stderr)
            print('=' * 50, file=sys.stderr)
            return

        status(f'🚀 Creating GitHub release for v{version}...')
        status(f'   📎 Tag: {tag}')
        status(f'   🎯 Target: {commit}')
        status(f'   📝 Title: {title}')

        start_time = time.time()

        # Execute the command
        result = subprocess.run(cmd, capture_output=True, text=True)

        elapsed = time.time() - start_time

        if result.returncode != 0:
            print(f'❌ Error creating GitHub release (after {elapsed:.1f}s):', file=sys.stderr)
            print(f'   STDOUT: {result.stdout}', file=sys.stderr)
            print(f'   STDERR: {result.stderr}', file=sys.stderr)
            sys.exit(1)

        status(f'✅ GitHub release created successfully in {elapsed:.1f}s!')
        if result.stdout.strip():
            status(f'   🔗 {result.stdout.strip()}')

    finally:
        # Clean up temporary file
        try:
            os.unlink(notes_file)
        except OSError:
            pass


def check_prerequisites() -> None:
    """Check that required tools are available."""
    status('🔍 Checking prerequisites...')

    # Check if gh CLI is available
    try:
        result = subprocess.run(['gh', '--version'], capture_output=True, text=True, check=True)
        version = result.stdout.strip().split()[2]
        status(f'   ✓ GitHub CLI found: v{version}')
    except (subprocess.CalledProcessError, FileNotFoundError):
        print('❌ GitHub CLI (gh) is not installed or not in PATH', file=sys.stderr)
        print('   Please install it from https://cli.github.com/', file=sys.stderr)
        sys.exit(1)

    # Check if we're in a git repository
    try:
        subprocess.run(['git', 'rev-parse', '--git-dir'], capture_output=True, check=True)
        status('   ✓ Git repository detected')
    except subprocess.CalledProcessError:
        print('❌ Not in a git repository', file=sys.stderr)
        sys.exit(1)

    # Check if we're authenticated with GitHub
    try:
        result = subprocess.run(['gh', 'auth', 'status'], capture_output=True, text=True)
        if result.returncode != 0:
            print('❌ Not authenticated with GitHub', file=sys.stderr)
            print('   Please run: gh auth login', file=sys.stderr)
            sys.exit(1)
        else:
            # Extract username from output
            lines = result.stderr.split('\n')
            username = 'unknown'
            for line in lines:
                if 'Logged in to github.com as' in line:
                    username = line.split()[-1]
                    break
            status(f'   ✓ GitHub authenticated as {username}')
    except subprocess.CalledProcessError:
        print('❌ Could not check GitHub authentication status', file=sys.stderr)
        sys.exit(1)

    status('✅ All prerequisites satisfied')


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Create GitHub release for SingleStoreDB Python SDK',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''Examples:
  %(prog)s                        # Use the version on origin/main
  %(prog)s --version 1.15.6       # Check origin/main is at this version
  %(prog)s --dry-run               # Preview without executing''',
    )

    parser.add_argument(
        '--version',
        help='Version to release; must match pyproject.toml on origin/main (default: that version)',
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without executing',
    )

    args = parser.parse_args()

    try:
        total_start_time = time.time()

        print('🚀 Starting GitHub release creation', file=sys.stderr)
        print('=' * 50, file=sys.stderr)

        # Step 1: Check prerequisites (unless dry run)
        if not args.dry_run:
            step(1, 4, 'Checking prerequisites')
            check_prerequisites()
        else:
            step(1, 4, 'Skipping prerequisites check (dry-run)')

        # Step 2: Get version
        step(2, 4, 'Determining version')
        start_time = time.time()

        status(f'🔄 Fetching {RELEASE_REF}...')
        tip = fetch_release_ref()
        version = get_version_from_pyproject(tip)
        status(f'Extracted from pyproject.toml on {RELEASE_REF} ({tip[:12]}): {version}')

        if args.version and args.version != version:
            raise ValueError(
                f'{RELEASE_REF} is at version {version}, not {args.version}; '
                'has the release pull request been merged?',
            )

        check_tag_absent(f'v{version}')

        commit = find_release_commit(version)
        if commit != tip:
            later = git('rev-list', '--count', f'{commit}..{tip}').strip()
            status(f'Release commit is {commit[:12]}; {later} later commit(s) on {RELEASE_REF} are not included')
        if get_version_from_pyproject(commit) != version:
            raise ValueError(
                f'pyproject.toml at release commit {commit[:12]} is not at version {version}',
            )

        elapsed = time.time() - start_time
        status(f'✅ Version determined in {elapsed:.1f}s')

        # Step 3: Extract release notes
        step(3, 4, 'Extracting release notes')
        start_time = time.time()

        status(f'📄 Reading release notes for v{version}...')
        notes = extract_release_notes(version, commit)
        lines_count = len(notes.split('\n'))

        elapsed = time.time() - start_time
        status(f'✅ Extracted {lines_count} lines of release notes in {elapsed:.1f}s')

        # Step 4: Create the release
        step(4, 4, 'Creating GitHub release')
        create_release(version, notes, commit, dry_run=args.dry_run)

        total_elapsed = time.time() - total_start_time
        print('=' * 50, file=sys.stderr)
        if args.dry_run:
            print(f'🔍 Dry run completed in {total_elapsed:.1f}s', file=sys.stderr)
        else:
            print(f'🎉 GitHub release created successfully in {total_elapsed:.1f}s!', file=sys.stderr)
            print(f'🏷️  Version: v{version}', file=sys.stderr)

    except Exception as e:
        print(f'❌ Error: {e}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
