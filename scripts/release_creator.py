#!/usr/bin/env python

import re
import requests
import subprocess
import os

release_template = """
_rqlite_ is a lightweight, distributed relational database, which uses [SQLite](https://www.sqlite.org/) as its storage engine. rqlite provides an easy-to-use, fault-tolerant store for your most important relational data. You can learn a lot more about rqlite at [rqlite.io](https://www.rqlite.io).

Release {release} {release_specific_notes}. See the [CHANGELOG](https://github.com/rqlite/rqlite/blob/master/CHANGELOG.md) for full details on this release, and check out the _Assets_ section below for prebuilt binaries.

## Getting started
_Check out the [Quick Start guide](https://rqlite.io/docs/quick-start/)._

To download and run a single rqlite node follow the directions below. It's also very easy to run a rqlite cluster -- you can learn more by checking out the [documentation](https://rqlite.io/docs/clustering/).

If you wish to build rqlite from source, check out [this documentation](https://rqlite.io/docs/install-rqlite/building-from-source/).

### Docker
Run a single node as follows:
```
docker pull rqlite/rqlite
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
```homebrew rqlite```

### Windows
rqlite can be built for Windows, and Windows compatibility is ensured via [AppVeyor](https://www.appveyor.com/). However you may need to build a specific release yourself, though the top-of-tree build [is available for download](https://ci.appveyor.com/api/projects/otoolep/rqlite/artifacts/rqlite-latest-win64.zip?branch=master) from AppVeyor. Check out the [CI build for Windows](https://ci.appveyor.com/project/otoolep/rqlite) for more details. Please note that I do not control the build process in AppVeyor and you download and use those binaries at your own risk.
"""

def generate_release_notes(release, features):
        return release_template.format(release=release, release_specific_notes=features)

def validate_release_string(release_str):
    pattern = re.compile(r'^v\d+\.\d+\.\d+$')
    return pattern.match(release_str)

def get_release_string():
    while True:
        release_str = input("Enter a release string in the format vX.Y.Z: ")
        if validate_release_string(release_str):
            return release_str
        else:
            print("Invalid release string. Please try again.")

def get_github_token():
    token = input("Enter your GitHub Personal Access Token: ")
    return token

def get_release_features():
    notes = input("Enter release features: ")
    return notes

def create_github_release(release_str, token, notes):
    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github+json'
    }
    data = {
        'tag_name': release_str,
        'name': release_str,
        'body': notes
    }
    url = 'https://api.github.com/repos/rqlite/rqlite/releases'
    response = requests.post(url, headers=headers, json=data)
    if response.status_code != 201:
        response.raise_for_status()
    return response.json()['id']

def confirm_bash_command(command):
    print("\nAbout to run the following bash command:")
    print(" ".join(command))
    confirmation = input("\nDo you want to proceed? (yes/no): ")
    return confirmation.lower() == "yes"

def invoke_package_script(release_str, release_id, token):
    if os.path.exists('package.sh'):
        command = ['./package.sh', release_str, str(release_id), token]
        if confirm_bash_command(command):
            subprocess.run(command, check=True)
        else:
            print("Aborting program.")
            exit(1)
    else:
        print("package.sh not found. Please ensure it's in the current working directory.")

def confirm_CHANGELOG():
    confirmation = input("Have you dated the CHANGELOG?: ")
    return confirmation.lower() == "yes"

def confirm_release_notes(release_notes):
    print("\nRelease Notes:")
    print(release_notes)
    confirmation = input("\nDo the release notes look good? (yes/no): ")
    return confirmation.lower() == "yes"

def main():
    if os.getenv("GOBIN"):
        print("GOBIN is set, which will prevent cross-compilation. Please unset it and try again.")
        exit(1)

    while True:
        if not confirm_CHANGELOG():
            continue

        release_str = get_release_string()
        features = get_release_features()
        release_notes = generate_release_notes(release_str, features)
        
        if confirm_release_notes(release_notes):
            break
        else:
            print("Please enter the release string and release-specific notes again.\n")

    token = get_github_token()
    try:
        release_id = create_github_release(release_str, token, release_notes)
        print(f"Release created with ID: {release_id}")
        invoke_package_script(release_str, release_id, token)
    except requests.HTTPError as e:
        print(f"An error occurred while creating the release: {e}")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running package.sh: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
