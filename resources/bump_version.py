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
import time
from pathlib import Path


def status(message: str) -> None:
    """Show status messages to indicate progress."""
    print(f'📋 {message}', file=sys.stderr)


def step(step_num: int, total_steps: int, message: str) -> None:
    """Show a numbered step with progress."""
    print(f'📍 Step {step_num}/{total_steps}: {message}', file=sys.stderr)


def get_current_version() -> str:
    """Get the current version from pyproject.toml."""
    pyproject_path = Path(__file__).parent.parent / 'pyproject.toml'
    with open(pyproject_path, 'r') as f:
        content = f.read()

    match = re.search(r'^version\s*=\s*["\'](.+)["\']$', content, re.MULTILINE)
    if not match:
        raise ValueError('Could not find version in pyproject.toml')

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

    # For pyproject.toml
    if file_path.name == 'pyproject.toml':
        pattern = r'^(version\s*=\s*["\'])' + re.escape(old_version) + r'(["\'])$'
        replacement = r'\g<1>' + new_version + r'\g<2>'
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
    """Build the documentation using the unified build script."""
    build_script = Path(__file__).parent / 'build_docs.py'

    if build_script.exists():
        # Use the new unified build script
        status('📚 Building documentation with unified script...')
        result = subprocess.run([sys.executable, str(build_script), 'html'])
    else:
        # Fallback to make html if build script doesn't exist
        docs_src_path = Path(__file__).parent.parent / 'docs' / 'src'
        status('📚 Building documentation with make...')
        result = subprocess.run(['make', 'html'], cwd=docs_src_path)

    if result.returncode != 0:
        print('❌ Error building documentation', file=sys.stderr)
        sys.exit(1)

    status('✅ Documentation built successfully')


def stage_files() -> None:
    """Stage all modified files for commit."""
    status('📦 Staging files for commit...')

    files_to_stage = [
        'pyproject.toml',
        'singlestoredb/__init__.py',
        'docs/src/whatsnew.rst',
        'docs/',  # All generated documentation files
    ]

    for file_path in files_to_stage:
        subprocess.run(['git', 'add', file_path], check=True)
        status(f'   ✓ Staged {file_path}')

    status('✅ All modified files staged for commit')


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

    total_start_time = time.time()

    print('🚀 Starting version bump process', file=sys.stderr)
    print('=' * 50, file=sys.stderr)

    # Step 1: Get current version
    step(1, 6, 'Reading current version')
    current_version = get_current_version()
    status(f'Current version: {current_version}')

    # Calculate new version
    new_version = bump_version(current_version, args.bump_type)
    status(f'New version will be: {new_version}')

    # Step 2: Update version in files
    step(2, 6, 'Updating version in files')
    start_time = time.time()

    update_version_in_file(Path(__file__).parent.parent / 'pyproject.toml', current_version, new_version)
    status('   ✓ Updated pyproject.toml')

    update_version_in_file(
        Path(__file__).parent.parent / 'singlestoredb' / '__init__.py',
        current_version,
        new_version,
    )
    status('   ✓ Updated singlestoredb/__init__.py')

    elapsed = time.time() - start_time
    status(f'✅ Version files updated in {elapsed:.1f}s')

    # Step 3: Generate release summary
    step(3, 6, 'Generating release summary')
    start_time = time.time()

    if args.summary:
        status('Using provided summary')
        # Replace literal \n with actual newlines
        summary = args.summary.replace('\\n', '\n')
    else:
        status('🔍 Analyzing git history...')
        git_log = get_git_log_since_last_release(current_version)
        summary = summarize_changes(git_log)

    elapsed = time.time() - start_time
    status(f'✅ Release summary generated in {elapsed:.1f}s')

    # Step 4: Update whatsnew.rst with editor
    step(4, 6, 'Updating release notes')
    start_time = time.time()

    status('📝 Opening editor for release notes...')
    if not update_whatsnew_with_editor(new_version, summary):
        print('\n❌ Operation cancelled by user', file=sys.stderr)
        status('🔄 Reverting version changes...')

        # Revert version changes
        update_version_in_file(Path(__file__).parent.parent / 'pyproject.toml', new_version, current_version)
        update_version_in_file(
            Path(__file__).parent.parent / 'singlestoredb' / '__init__.py',
            new_version,
            current_version,
        )

        status('✅ Version changes reverted')
        sys.exit(1)

    elapsed = time.time() - start_time
    status(f'✅ Release notes updated in {elapsed:.1f}s')

    # Step 5: Build documentation
    step(5, 6, 'Building documentation')
    build_docs()

    # Step 6: Stage files
    step(6, 6, 'Staging files for commit')
    stage_files()

    total_elapsed = time.time() - total_start_time
    print('=' * 50, file=sys.stderr)
    print(f'🎉 Version bump completed successfully in {total_elapsed:.1f}s!', file=sys.stderr)
    print(f'📝 Version: {current_version} → {new_version}', file=sys.stderr)
    print('🚀 Next steps:', file=sys.stderr)
    print('    📄 git commit -m "Prepare for v{} release" && git push'.format(new_version), file=sys.stderr)
    print('    📄 Run Coverage tests <https://github.com/singlestore-labs/singlestoredb-python/actions/workflows/coverage.yml>', file=sys.stderr)
    print('    📄 Run Smoke test <https://github.com/singlestore-labs/singlestoredb-python/actions/workflows/smoke-test.yml>', file=sys.stderr)
    print('    📄 Run resources/create_release.py', file=sys.stderr)


if __name__ == '__main__':
    main()
