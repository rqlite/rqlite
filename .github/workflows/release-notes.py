#!/usr/bin/env python

import sys
import requests

release_template = """
_rqlite_ is a lightweight, user-friendly, distributed relational database, which uses [SQLite](https://www.sqlite.org/) as its storage engine. rqlite provides an easy-to-use, fault-tolerant and highly-available store for your most important relational data. You can learn a lot more about rqlite at [rqlite.io](https://www.rqlite.io).

See the [CHANGELOG](https://github.com/rqlite/rqlite/blob/master/CHANGELOG.md) for full details on {release}, and check out the _Assets_ section below for prebuilt binaries.

## Getting started
_Check out the [Quick Start guide](https://rqlite.io/docs/quick-start/)._

To download and run a single rqlite node follow the directions below. It's also very easy to run a rqlite cluster -- you can learn more by checking out the [documentation](https://rqlite.io/docs/clustering/).

If you wish to build rqlite from source, check out [this documentation](https://rqlite.io/docs/install-rqlite/building-from-source/).

### Docker
Run a single node as follows:
```
docker run -p4001:4001 rqlite/rqlite
```

Check out the [rqlite Docker page](https://hub.docker.com/r/rqlite/rqlite/) for more details on running nodes via Docker.

### Linux
_Builds for a variety of CPU architectures are available. See the Assets section below._

To download and start rqlite, execute the following in a shell.

```
curl -L https://github.com/rqlite/rqlite/releases/download/{release}/rqlite-{release}-linux-amd64.tar.gz -o rqlite-{release}-linux-amd64.tar.gz
tar xvfz rqlite-{release}-linux-amd64.tar.gz
cd rqlite-{release}-linux-amd64
./rqlited ~/node.1
```

### macOS
Install via [Homebrew](https://formulae.brew.sh/formula/rqlite).

```brew install rqlite```

### Windows
rqlite can be built for Windows, and Windows compatibility is ensured via [AppVeyor](https://www.appveyor.com/). You can download a pre-built release for Windows in one of two ways:
- The top-of-tree build [is available for download](https://ci.appveyor.com/api/projects/otoolep/rqlite/artifacts/rqlite-latest-win64.zip?branch=master) from AppVeyor. Check out the [CI build for Windows](https://ci.appveyor.com/project/otoolep/rqlite) for more details.
- Download the Win64 [build artifact attached to this release](https://github.com/rqlite/rqlite/releases/download/{release}/rqlite-{release}-win64.zip). This build is currently considered experimental.
"""

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
    if len(sys.argv) != 4 or not all(sys.argv[1:]):
        print("Usage: python release-notes.py <GITHUB_TOKEN> <RELEASE ID> <TAG>")
        sys.exit(1)

    token = sys.argv[1]
    release_id = sys.argv[2]
    tag = sys.argv[3]
    set_release_notes(token, release_id, release_template.format(release=tag))

if __name__ == "__main__":
    main()
