<picture>
 <source media="(prefers-color-scheme: light)" srcset="DOC/logo-text.png">
  <source media="(prefers-color-scheme: dark)" srcset="DOC/logo-text-dark.png">
 <img alt="rqlite logo" src="DOC/logo-text.png" height=100>
</picture>

[![Circle CI](https://circleci.com/gh/rqlite/rqlite/tree/master.svg?style=svg)](https://circleci.com/gh/rqlite/rqlite/tree/master)
[![AppVeyor](https://ci.appveyor.com/api/projects/status/github/rqlite/rqlite?branch=master&svg=true)](https://ci.appveyor.com/project/otoolep/rqlite)
[![Go Report Card](https://goreportcard.com/badge/github.com/rqlite/rqlite)](https://goreportcard.com/report/github.com/rqlite/rqlite/v8)
[![Release](https://img.shields.io/github/release/rqlite/rqlite.svg)](https://github.com/rqlite/rqlite/releases)
[![Docker](https://img.shields.io/docker/pulls/rqlite/rqlite?style=plastic)](https://hub.docker.com/r/rqlite/rqlite/)
[![Office Hours](https://img.shields.io/badge/Office%20Hours--yellow.svg)](https://rqlite.io/office-hours)
[![Slack](https://img.shields.io/badge/Slack--purple.svg)](https://www.rqlite.io/join-slack)
[![Google Group](https://img.shields.io/badge/Google%20Group--blue.svg)](https://groups.google.com/group/rqlite)

[rqlite](https://rqlite.io) is a [rock](https://www.sqlite.org/testing.html)-[solid](https://philipotoole.com/how-is-rqlite-tested/), highly-available, distributed relational database built on [SQLite](https://www.sqlite.org/). It's lightweight, developer-friendly, and exceptionally easy to operate.

Use rqlite to reliably store your most important data, ensuring it's always available to your applications. Whether you're deploying **resilient services in the cloud** or **reliable applications at the edge**, rqlite is a solution that offers effortless installation, deployment, and operation.

## Quick Start
[_Check out the full Quick Start guide_](https://rqlite.io/docs/quick-start/)

**1. Run a rqlite node:**
```bash
docker run -p 4001:4001 rqlite/rqlite
```

**2. Create a table and insert a row:**
```bash
curl -XPOST 'localhost:4001/db/execute' -H 'Content-Type: application/json' -d '[
     "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)",
     "INSERT INTO foo(id, name) VALUES(1, \"fiona\")"
]'
```

**3. Query the data:**
```bash
curl -G 'localhost:4001/db/query' --data-urlencode 'q=SELECT * FROM foo'
```

Now you have a single-node running. [Learn how to form a multi-node cluster in seconds.](https://rqlite.io/docs/clustering/) and dive into the [_Developer Guide_](https://www.rqlite.io/docs/api).

## Key features

**Core functionality**
- **Relational**: Full SQL support via SQLite, including [Full-text search](https://www.sqlite.org/fts5.html), JSON support, and more.
- **Extensible**: [Load SQLite extensions](https://rqlite.io/docs/guides/extensions/) to add capabilities like [Vector Search](https://github.com/asg017/sqlite-vec) and [Crypto](https://github.com/nalgeon/sqlean).
- **Atomic Requests**: Execute multiple SQL statements atomically within a single API request.

**Easy Operations**
- **Easy Deployment**: A single binary with no external dependencies. Up and running in seconds.
- **High Availability**: Fully replicated database provides fault-tolerance. Outage of a node doesn't impact the cluster.
- **Dynamic Clustering**: Automatic clustering via [Kubernetes](https://rqlite.io/docs/guides/kubernetes/), [Docker Compose](https://rqlite.io/docs/guides/docker-compose/), Consul, etcd, or DNS.
- **Effortless Backups**: Hot [backups](https://rqlite.io/docs/guides/backup/), including [automatic backups to AWS S3, MinIO, and Google Cloud](https://rqlite.io/docs/guides/backup/#automatic-backups), as well as [restore directly from SQLite](https://rqlite.io/docs/guides/backup/#restoring-from-sqlite) and Cloud-based storage.

**Developer Experience**
- **Simple APIs**: Easy-to-use [HTTP API](https://rqlite.io/docs/api/), [CLI](https://rqlite.io/docs/cli/) and official/community [client libraries](https://rqlite.io/docs/api/client-libraries/).
- **Robust Security**: [End-to-end encryption with TLS](https://rqlite.io/docs/guides/security/) and rich authentication/authorization controls.
- **Tunable Consistency**: Customize [read consistency](https://rqlite.io/docs/api/read-consistency/) and [durability](https://rqlite.io/docs/api/queued-writes/) to match your application's needs.

## More questions?
- [Join the rqlite Slack channel](https://rqlite.io/join-slack)
- [Sign up for Office Hours](https://rqlite.io/office-hours)

## Pronunciation
Common pronunciations of rqlite include "R Q lite" and "ree-qwell-lite".
