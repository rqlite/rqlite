#!/usr/bin/env python

import re
import sys
from pathlib import Path

import requests

CHANGELOG_PATH = Path(__file__).resolve().parents[2] / 'CHANGELOG.md'

release_template = """
_rqlite_ is a lightweight, user-friendly, distributed relational database, which uses [SQLite](https://www.sqlite.org/) as its storage engine. rqlite provides easy-to-use, fault-tolerant and highly-available storage for your most important data. You can learn a lot more about rqlite at [rqlite.io](https://www.rqlite.io).

{notes}

## Getting started
_Check out the [Quick Start guide](https://rqlite.io/docs/quick-start/)._

To download and run a single rqlite node follow the directions below. It's also very easy to run a rqlite cluster -- you can learn more by checking out the [documentation](https://rqlite.io/docs/clustering/).

If you wish to build rqlite from source, check out [this documentation](https://rqlite.io/docs/install-rqlite/building-from-source/).

### Docker
_In addition to Docker Hub, images are also uploaded to [ghcr.io](https://github.com/rqlite/rqlite/pkgs/container/rqlite)._

Run a single node as follows:
```
docker run -p4001:4001 rqlite/rqlite
```

Check out the [rqlite Docker page](https://hub.docker.com/r/rqlite/rqlite/) for more details on running rqlite via Docker, including how to deploy a cluster.

### Linux
_Builds for a variety of CPU architectures are available. See the Assets section below if you wish to download and install manually._

The easiest way to install rqlite on Linux:
```
curl -fsSL https://rqlite.io/install.sh -o install.sh
sh install.sh
```
To install a specific version to a specific location:
```
curl -fsSL https://rqlite.io/install.sh -o install.sh
RQLITE_VERSION={release} INSTALL_DIR={{location}} sh install.sh
```

### Packages
_.deb and .rpm packages for amd64 and arm64 are available in the Assets section below._

Install on Debian/Ubuntu:
```
sudo dpkg -i rqlite_{version}_amd64.deb
sudo systemctl start rqlited
```

Install on RHEL/Fedora:
```
sudo rpm -i rqlite-{version}-1.x86_64.rpm
sudo systemctl start rqlited
```

### macOS
Install via [Homebrew](https://formulae.brew.sh/formula/rqlite).

```brew install rqlite```

### Windows
You can download a pre-built release for Windows in one of two ways:
- The top-of-tree build [is available for download](https://ci.appveyor.com/api/projects/otoolep/rqlite/artifacts/rqlite-latest-win64.zip?branch=master) from AppVeyor. Check out the [CI build for Windows](https://ci.appveyor.com/project/otoolep/rqlite) for more details.
- Download the Win64 [build artifact attached to this release](https://github.com/rqlite/rqlite/releases/download/{release}/rqlite-{release}-win64.zip).
"""

notes_template = """## What's changed
{section}

See the [CHANGELOG](https://github.com/rqlite/rqlite/blob/master/CHANGELOG.md) for the full history of changes, and check out the _Assets_ section below for prebuilt binaries."""

notes_fallback = """See the [CHANGELOG](https://github.com/rqlite/rqlite/blob/master/CHANGELOG.md) for full details on {release}, and check out the _Assets_ section below for prebuilt binaries."""

def changelog_section(text, tag):
    """Return the body of the CHANGELOG section for tag, or None if absent.

    A section starts at a level-2 heading whose first word is the tag,
    e.g. '## v10.6.6 (October 28th 2026)', and ends at the next level-2
    heading. Lines inside fenced code blocks are never treated as headings.
    """
    heading = re.compile(r'^##\s+(\S+)')
    body, in_section, in_fence = [], False, False
    for line in text.splitlines():
        if line.lstrip().startswith(('```', '~~~')):
            in_fence = not in_fence
        m = None if in_fence else heading.match(line)
        if m:
            if in_section:
                break
            in_section = m.group(1) == tag
            continue
        if in_section:
            body.append(line)
    if not in_section:
        return None
    section = '\n'.join(body).strip()
    return section or None

def release_notes(tag, changelog_path=CHANGELOG_PATH):
    """Render the release body for tag.

    The tag's CHANGELOG section is included when it can be found. Otherwise
    the body just links to the CHANGELOG, so a missing or renamed section
    never fails the release.
    """
    section = None
    try:
        section = changelog_section(changelog_path.read_text(encoding='utf-8'), tag)
    except OSError as e:
        print(f'warning: could not read {changelog_path}: {e}', file=sys.stderr)
    if section is None:
        print(f'warning: no CHANGELOG section found for {tag}', file=sys.stderr)
        notes = notes_fallback.format(release=tag)
    else:
        notes = notes_template.format(section=section)
    return release_template.format(release=tag, version=tag.lstrip('v'), notes=notes)

def set_release_notes(token, release_id, notes):
    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github+json'
    }
    data = {
        'body': notes
    }
    url = 'https://api.github.com/repos/rqlite/rqlite/releases/' + str(release_id)
    response = requests.patch(url, headers=headers, json=data)
    if response.status_code != 200:
        response.raise_for_status()
    return response.json()['id']

def main():
    if len(sys.argv) == 3 and sys.argv[1] == '--dry-run' and sys.argv[2]:
        print(release_notes(sys.argv[2]))
        return

    if len(sys.argv) != 4 or not all(sys.argv[1:]):
        print("Usage: python release-notes.py <GITHUB_TOKEN> <RELEASE ID> <TAG>")
        print("       python release-notes.py --dry-run <TAG>")
        sys.exit(1)

    token = sys.argv[1]
    release_id = sys.argv[2]
    tag = sys.argv[3]
    set_release_notes(token, release_id, release_notes(tag))

if __name__ == "__main__":
    main()
