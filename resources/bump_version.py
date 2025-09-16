#!/usr/bin/env python3
"""
Command for bumping version and generating documentation.

Usage: python bump_version.py [major|minor|patch] [--summary "Release notes"]

Examples:
    python bump_version.py patch
    python bump_version.py minor --summary "* Added new feature X\n* Fixed bug Y"
    python bump_version.py major --summary "Breaking changes:\n* Removed deprecated API"

Note: Release notes should be in reStructuredText format.

"""
from __future__ import annotations

import argparse
import datetime
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path


def get_current_version() -> str:
    """Get the current version from setup.cfg."""
    setup_cfg_path = Path(__file__).parent.parent / 'setup.cfg'
    with open(setup_cfg_path, 'r') as f:
        content = f.read()

    match = re.search(r'^version\s*=\s*(.+)$', content, re.MULTILINE)
    if not match:
        raise ValueError('Could not find version in setup.cfg')

    return match.group(1).strip()


def bump_version(current_version: str, bump_type: str) -> str:
    """Bump the version number based on the bump type."""
    parts = current_version.split('.')
    major = int(parts[0])
    minor = int(parts[1])
    patch = int(parts[2])

    if bump_type == 'major':
        major += 1
        minor = 0
        patch = 0
    elif bump_type == 'minor':
        minor += 1
        patch = 0
    elif bump_type == 'patch':
        patch += 1
    else:
        raise ValueError(f'Invalid bump type: {bump_type}')

    return f'{major}.{minor}.{patch}'


def update_version_in_file(file_path: Path, old_version: str, new_version: str) -> None:
    """Update version in a file."""
    with open(file_path, 'r') as f:
        content = f.read()

    # For setup.cfg
    if file_path.name == 'setup.cfg':
        pattern = r'^(version\s*=\s*)' + re.escape(old_version) + r'$'
        replacement = r'\g<1>' + new_version
        content = re.sub(pattern, replacement, content, flags=re.MULTILINE)

    # For __init__.py
    elif file_path.name == '__init__.py':
        pattern = r"^(__version__\s*=\s*['\"])" + re.escape(old_version) + r"(['\"])$"
        replacement = r'\g<1>' + new_version + r'\g<2>'
        content = re.sub(pattern, replacement, content, flags=re.MULTILINE)

    with open(file_path, 'w') as f:
        f.write(content)


def get_git_log_since_last_release(current_version: str) -> str:
    """Get git commits since the last release."""
    # Get the tag for the current version (which should be the last release)
    try:
        # Try to find the last tag that matches a version pattern
        result = subprocess.run(
            ['git', 'tag', '-l', '--sort=-version:refname', 'v*'],
            capture_output=True,
            text=True,
            check=True,
        )
        tags = result.stdout.strip().split('\n')

        # Find the tag matching the current version or the most recent tag
        current_tag = f'v{current_version}'
        if current_tag in tags:
            last_tag = current_tag
        elif tags:
            last_tag = tags[0]
        else:
            # If no tags found, get all commits
            last_tag = None
    except subprocess.CalledProcessError:
        last_tag = None

    # Get commits since last tag
    if last_tag:
        cmd = ['git', 'log', f'{last_tag}..HEAD', '--oneline', '--no-merges']
    else:
        # Get last 20 commits if no tag found
        cmd = ['git', 'log', '--oneline', '--no-merges', '-20']

    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return result.stdout.strip()


def summarize_changes(git_log: str) -> str:
    """Summarize the git log into categories."""
    lines = git_log.split('\n')
    if not lines or not lines[0]:
        return 'No changes since last release.'

    features = []
    fixes = []
    other = []

    for line in lines:
        if not line:
            continue

        # Remove commit hash
        parts = line.split(' ', 1)
        if len(parts) > 1:
            message = parts[1]
        else:
            continue

        # Categorize based on commit message
        lower_msg = message.lower()
        if any(word in lower_msg for word in ['add', 'feat', 'feature', 'implement', 'new']):
            features.append(message)
        elif any(word in lower_msg for word in ['fix', 'bug', 'patch', 'correct', 'resolve']):
            fixes.append(message)
        else:
            other.append(message)

    summary = []

    if features:
        # summary.append('**New Features:**')
        for feat in features[:5]:  # Limit to 5 most recent
            summary.append(f'* {feat}')
        if len(features) > 5:
            summary.append(f'* ...and {len(features) - 5} more features')
        # summary.append('')

    if fixes:
        # summary.append('**Bug Fixes:**')
        for fix in fixes[:5]:  # Limit to 5 most recent
            summary.append(f'* {fix}')
        if len(fixes) > 5:
            summary.append(f'* ...and {len(fixes) - 5} more fixes')
        # summary.append('')

    if other:
        # summary.append('**Other Changes:**')
        for change in other[:3]:  # Limit to 3 most recent
            summary.append(f'* {change}')
        if len(other) > 3:
            summary.append(f'* ...and {len(other) - 3} more changes')

    return '\n'.join(summary) if summary else '* Various improvements and updates'


def edit_content(content: str, description: str = 'content') -> str | None:
    """Open the default editor to edit content and return the edited result.

    Args:
        content: The initial content to edit
        description: Description of what's being edited (for messages)

    Returns:
        The edited content, or None if the user cancelled (empty content)
    """
    # Get the editor from environment variables
    editor = os.environ.get('EDITOR') or os.environ.get('VISUAL') or 'vi'

    # Create a temporary file with the content
    with tempfile.NamedTemporaryFile(mode='w+', suffix='.txt', delete=False) as tmp_file:
        tmp_file.write(content)
        tmp_file.flush()
        tmp_path = tmp_file.name

    try:
        print(f'\nOpening editor to edit {description}...')
        print(f'Editor: {editor}')
        print('Save and exit to continue, or clear all content to cancel.')

        # Open the editor
        result = subprocess.run([editor, tmp_path])

        if result.returncode != 0:
            print(f'Editor exited with non-zero status: {result.returncode}')
            return None

        # Read the edited content
        with open(tmp_path, 'r') as f:
            edited_content = f.read().strip()

        if not edited_content:
            print('Content is empty - cancelling operation.')
            return None

        return edited_content

    finally:
        # Clean up the temporary file
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def prepare_whatsnew_content(new_version: str, summary: str) -> str:
    """Prepare the content for the new release section."""
    today = datetime.date.today()
    date_str = today.strftime('%B %d, %Y').replace(' 0', ' ')  # Remove leading zero

    new_section = f'v{new_version} - {date_str}\n'
    new_section += '-' * len(new_section.strip()) + '\n'
    new_section += summary.strip()

    return new_section


def update_whatsnew_with_editor(new_version: str, summary: str) -> bool:
    """Update the whatsnew.rst file with the new release, allowing user to edit content.

    Returns:
        True if successful, False if cancelled by user
    """
    whatsnew_path = Path(__file__).parent.parent / 'docs' / 'src' / 'whatsnew.rst'

    # Prepare the initial content for the new release
    new_release_content = prepare_whatsnew_content(new_version, summary)

    # Let the user edit the content
    edited_content = edit_content(new_release_content, 'release notes')
    if edited_content is None:
        return False

    # Read the current whatsnew.rst file
    with open(whatsnew_path, 'r') as f:
        content = f.read()

    # Find the position after the note section
    note_end = content.find('\n\nv')
    if note_end == -1:
        # If no versions found, add after the document description
        note_end = content.find('changes to the API.\n') + len('changes to the API.\n')

    # Insert the new section
    content = content[:note_end] + '\n\n' + edited_content + content[note_end:]

    with open(whatsnew_path, 'w') as f:
        f.write(content)

    return True


def build_docs() -> None:
    """Build the documentation."""
    docs_src_path = Path(__file__).parent.parent / 'docs' / 'src'

    # Change to docs/src directory
    original_dir = os.getcwd()
    os.chdir(docs_src_path)

    try:
        # Run make html
        result = subprocess.run(['make', 'html'], capture_output=True, text=True)
        if result.returncode != 0:
            print('Error building documentation:')
            print(result.stderr)
            sys.exit(1)
        print('Documentation built successfully')
    finally:
        # Change back to original directory
        os.chdir(original_dir)


def stage_files() -> None:
    """Stage all modified files for commit."""
    # Stage version files
    subprocess.run(['git', 'add', 'setup.cfg'], check=True)
    subprocess.run(['git', 'add', 'singlestoredb/__init__.py'], check=True)
    subprocess.run(['git', 'add', 'docs/src/whatsnew.rst'], check=True)

    # Stage any generated documentation files
    subprocess.run(['git', 'add', 'docs/'], check=True)

    print('\nAll modified files have been staged for commit.')
    print("You can now commit with: git commit -m 'Bump version to X.Y.Z'")


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Bump version and generate documentation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''Examples:
  %(prog)s patch
  %(prog)s minor --summary "* Added new feature X\\n* Fixed bug Y"
  %(prog)s major --summary "Breaking changes:\\n* Removed deprecated API"''',
    )
    parser.add_argument(
        'bump_type',
        choices=['major', 'minor', 'patch'],
        help='Type of version bump',
    )
    parser.add_argument(
        '--summary',
        default=None,
        help='Optional summary for the release notes (supports reStructuredText and \\n for newlines)',
    )

    args = parser.parse_args()

    # Get current version
    current_version = get_current_version()
    print(f'Current version: {current_version}')

    # Calculate new version
    new_version = bump_version(current_version, args.bump_type)
    print(f'New version: {new_version}')

    # Update version in files
    print('\nUpdating version in files...')
    update_version_in_file(Path(__file__).parent.parent / 'setup.cfg', current_version, new_version)
    update_version_in_file(
        Path(__file__).parent.parent / 'singlestoredb' / '__init__.py',
        current_version,
        new_version,
    )

    # Get summary - either from argument or from git history
    if args.summary:
        print('\nUsing provided summary...')
        # Replace literal \n with actual newlines
        summary = args.summary.replace('\\n', '\n')
    else:
        print('\nAnalyzing git history...')
        git_log = get_git_log_since_last_release(current_version)
        summary = summarize_changes(git_log)

    # Update whatsnew.rst with editor
    print('\nUpdating whatsnew.rst...')
    if not update_whatsnew_with_editor(new_version, summary):
        print('\n❌ Operation cancelled by user')
        print('Reverting version changes...')

        # Revert version changes
        update_version_in_file(Path(__file__).parent.parent / 'setup.cfg', new_version, current_version)
        update_version_in_file(
            Path(__file__).parent.parent / 'singlestoredb' / '__init__.py',
            new_version,
            current_version,
        )

        print('Version changes reverted.')
        sys.exit(1)

    # Build documentation
    print('\nBuilding documentation...')
    build_docs()

    # Stage files
    print('\nStaging files for commit...')
    stage_files()

    print(f'\n✅ Version bumped from {current_version} to {new_version}')
    print('✅ Documentation updated and built')
    print('✅ Files staged for commit')


if __name__ == '__main__':
    main()
