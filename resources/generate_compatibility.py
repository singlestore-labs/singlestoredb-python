#!/usr/bin/env python3
"""Generate the driver, server, and Python compatibility matrix."""
from __future__ import annotations

import argparse
import datetime
import json
import re
import subprocess
import sys
import urllib.request
from collections.abc import Sequence
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple


ROOT = Path(__file__).parent.parent
OUTPUT_PATH = ROOT / 'COMPATIBILITY.md'
WHATSNEW_PATH = ROOT / 'docs' / 'src' / 'whatsnew.rst'
WORKFLOW_PATH = '.github/workflows/smoke-test.yml'
VERSION_PATTERN = re.compile(r'^v(\d+\.\d+\.\d+) - (.+)$', re.MULTILINE)
ROW_VERSION_PATTERN = re.compile(r'^\d+\.\d+\.\d+$')
EOL_PAGE_URL = (
    'https://docs.singlestore.com/db/latest/support/'
    'singlestore-software-end-of-life-eol-policy/'
)
RC_VERSIONS_URL = (
    'https://release.memsql.com/rc/index/singlestoredbserver/latest.json'
)
HEADER = [
    'Driver Version',
    'Release date',
    'Engine versions in support',
    'Tested Python versions',
]
EngineRelease = Tuple[str, datetime.date, datetime.date]


def parse_date(value: str) -> datetime.date:
    """Parse either an ISO date or a release-note date."""
    value = value.split('(', 1)[0].strip()
    try:
        return datetime.date.fromisoformat(value)
    except ValueError:
        return datetime.datetime.strptime(
            value.replace(',', ''),
            '%B %d %Y',
        ).date()


def version_key(version: str) -> Tuple[int, ...]:
    """Return a sortable key for a dotted version."""
    return tuple(int(item) for item in version.split('.'))


def get_current_version() -> str:
    """Read the current package version."""
    content = (ROOT / 'pyproject.toml').read_text()
    match = re.search(
        r'^version\s*=\s*["\'](.+)["\']$',
        content,
        re.MULTILINE,
    )
    if not match:
        raise ValueError('Could not find version in pyproject.toml')
    return match.group(1)


def get_client_releases() -> List[Tuple[str, datetime.date]]:
    """Read stable client releases and dates from the changelog."""
    releases: Dict[str, datetime.date] = {}
    for match in VERSION_PATTERN.finditer(WHATSNEW_PATH.read_text()):
        version = match.group(1)
        if version_key(version)[0] < 1 or version in releases:
            continue
        releases[version] = parse_date(match.group(2))
    return sorted(
        releases.items(),
        key=lambda item: version_key(item[0]),
        reverse=True,
    )


def read_existing_rows() -> Dict[str, List[str]]:
    """Read the rows already published in COMPATIBILITY.md."""
    if not OUTPUT_PATH.exists():
        return {}
    rows: Dict[str, List[str]] = {}
    for line in OUTPUT_PATH.read_text().splitlines():
        if not line.startswith('|'):
            continue
        cells = [cell.strip() for cell in line.strip().strip('|').split('|')]
        if len(cells) == len(HEADER) and ROW_VERSION_PATTERN.match(cells[0]):
            rows[cells[0]] = cells
    return rows


def read_tagged_workflow(version: str) -> Optional[str]:
    """Read the smoke-test workflow at a release tag, if the tag exists."""
    result = subprocess.run(
        ['git', 'show', f'v{version}:{WORKFLOW_PATH}'],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout if result.returncode == 0 else None


def read_workflow(version: str, current_version: str) -> str:
    """Read the smoke-test workflow used by a release."""
    workflow = read_tagged_workflow(version)
    if workflow is not None:
        return workflow
    if version == current_version:
        return (ROOT / WORKFLOW_PATH).read_text()
    raise RuntimeError(f'Could not read {WORKFLOW_PATH} from tag v{version}')


def get_tested_python_versions(workflow: str) -> List[str]:
    """Extract the Python matrix from a smoke-test workflow."""
    lines = workflow.splitlines()
    for index, line in enumerate(lines):
        key, _, value = line.strip().partition(':')
        if key != 'python-version':
            continue

        value = value.strip()
        if value.startswith('['):
            versions = re.findall(r'3\.\d+', value)
            if versions:
                return versions
            continue
        if value:
            continue

        indentation = len(line) - len(line.lstrip())
        versions = []
        for item in lines[index + 1:]:
            item_indentation = len(item) - len(item.lstrip())
            if item.strip() and item_indentation <= indentation:
                break
            match = re.match(r'\s*-\s+["\']?(3\.\d+)["\']?\s*$', item)
            if match:
                versions.append(match.group(1))
        if versions:
            return versions

    raise ValueError('Could not find the Python matrix in smoke-test.yml')


def format_python_versions(versions: Sequence[str]) -> str:
    """Format a Python version list as a compact range when possible."""
    minors = [int(version.split('.')[1]) for version in versions]
    if minors == list(range(minors[0], minors[-1] + 1)):
        return f'{versions[0]}–{versions[-1]}'
    return ', '.join(versions)


def fetch_bytes(url: str) -> bytes:
    """Fetch a URL used to build the compatibility matrix."""
    request = urllib.request.Request(
        url,
        headers={'User-Agent': 'singlestoredb-python-compatibility'},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read()


class _EOLTableParser(HTMLParser):
    """Extract the first engine lifecycle table from the EOL policy page."""

    def __init__(self) -> None:
        super().__init__()
        self.rows: List[List[str]] = []
        self._in_table = False
        self._table_done = False
        self._in_cell = False
        self._row: List[str] = []
        self._cell: List[str] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: List[Tuple[str, str | None]],
    ) -> None:
        attributes = dict(attrs)
        if (
            tag == 'table'
            and not self._table_done
            and 'table-component' in (attributes.get('class') or '')
        ):
            self._in_table = True
        elif self._in_table and tag == 'tr':
            self._row = []
        elif self._in_table and tag in ('td', 'th'):
            self._in_cell = True
            self._cell = []

    def handle_endtag(self, tag: str) -> None:
        if tag == 'table' and self._in_table:
            self._in_table = False
            self._table_done = True
        elif self._in_table and tag in ('td', 'th') and self._in_cell:
            self._row.append(''.join(self._cell).strip())
            self._in_cell = False
        elif self._in_table and tag == 'tr' and self._row:
            self.rows.append(self._row)

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._cell.append(data)


def fetch_engine_releases() -> List[EngineRelease]:
    """Read engine release and EOL dates from the EOL policy page."""
    parser = _EOLTableParser()
    parser.feed(fetch_bytes(EOL_PAGE_URL).decode('utf-8', 'replace'))
    releases = []
    for row in parser.rows:
        if len(row) < 3 or row[0] == 'Version':
            continue
        try:
            releases.append((
                row[0].split(' v', 1)[-1],
                datetime.datetime.strptime(row[1], '%B %d, %Y').date(),
                datetime.datetime.strptime(row[2], '%B %d, %Y').date(),
            ))
        except ValueError as exc:
            raise RuntimeError(
                f'Could not parse engine row {row} from {EOL_PAGE_URL}',
            ) from exc
    if not releases:
        raise RuntimeError(f'Could not parse engine versions from {EOL_PAGE_URL}')
    return releases


def fetch_rc_version() -> str:
    """Return the latest release candidate as major.minor."""
    data = json.loads(fetch_bytes(RC_VERSIONS_URL).decode('utf-8'))
    version = str(data.get('version') or '')
    parts = version.split('.')
    if len(parts) >= 2:
        return f'{parts[0]}.{parts[1]}'
    return version


def engines_supported_on(
    as_of: datetime.date,
    engines: Sequence[EngineRelease],
    rc_version: Optional[str],
) -> List[str]:
    """Return engines inside their support window on a client release date."""
    supported = [
        version
        for version, release_date, end_of_life in engines
        if release_date <= as_of and end_of_life > as_of
    ]
    if rc_version and rc_version not in supported:
        supported.append(f'{rc_version} RC')
    return sorted(supported, key=lambda version: version_key(version.split()[0]))


def format_table(rows: Sequence[Sequence[str]]) -> str:
    """Render an aligned Markdown table."""
    widths = [
        max(len(row[index]) for row in rows)
        for index in range(len(rows[0]))
    ]

    def render(row: Sequence[str]) -> str:
        cells = [
            value.ljust(width)
            for value, width in zip(row, widths)
        ]
        return '| ' + ' | '.join(cells) + ' |'

    separator = '| ' + ' | '.join('-' * width for width in widths) + ' |'
    return '\n'.join([render(rows[0]), separator, *map(render, rows[1:])])


def generate(rebuild: bool = False) -> str:
    """Generate the compatibility document."""
    current_version = get_current_version()
    existing = {} if rebuild else read_existing_rows()
    engines: Optional[List[EngineRelease]] = None
    rows = [HEADER]

    for version, release_date in get_client_releases():
        if version in existing:
            rows.append(existing[version])
            continue

        if engines is None:
            engines = fetch_engine_releases()
        rc_version = None
        if version == current_version and read_tagged_workflow(version) is None:
            rc_version = fetch_rc_version()

        workflow = read_workflow(version, current_version)
        rows.append([
            version,
            release_date.isoformat(),
            ', '.join(engines_supported_on(release_date, engines, rc_version)),
            format_python_versions(get_tested_python_versions(workflow)),
        ])

    table = format_table(rows)
    return f"""# Driver-Server Version Compatibility Matrix

<!-- Generated by resources/generate_compatibility.py; do not edit. -->

`resources/bump_version.py` adds a row for every client release. Published rows are kept as they are, so each row reflects what was known on that release date.

Engine versions in support come from the [SingleStore software EOL policy]({EOL_PAGE_URL}). An engine is listed when it had been released and its end-of-life date was still in the future on the client release date. These engine versions are not each tested in CI; the smoke tests run against a single managed SingleStore cluster. A version marked RC was the [latest release candidate]({RC_VERSIONS_URL}) when that client version was released.

Tested Python versions come from the smoke-test CI workflow at each release tag.

{table}
"""


def main() -> None:
    """Write the generated document or check that it is current."""
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        '--verify-matrix',
        action='store_true',
        help='fail if COMPATIBILITY.md is missing a release or is not current',
    )
    group.add_argument(
        '--rebuild',
        action='store_true',
        help='regenerate every row instead of keeping published rows',
    )
    args = parser.parse_args()

    content = generate(rebuild=args.rebuild)
    if args.verify_matrix:
        if not OUTPUT_PATH.exists() or OUTPUT_PATH.read_text() != content:
            print(
                'COMPATIBILITY.md is out of date; run '
                'python resources/generate_compatibility.py',
                file=sys.stderr,
            )
            sys.exit(1)
        return

    OUTPUT_PATH.write_text(content)


if __name__ == '__main__':
    main()
