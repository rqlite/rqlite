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

[*rqlite*](https://rqlite.io) is a developer-friendly and operator-centric database solution for [Linux, macOS, and Windows, as well as various CPU platforms](https://github.com/rqlite/rqlite/releases).

- [_Quick Start_ guide](https://rqlite.io/docs/quick-start/)
- [Developer guide](https://www.rqlite.io/docs/api)

_Check out the [rqlite FAQ](https://rqlite.io/docs/faq)_.

## Why run rqlite?
[rqlite](https://rqlite.io) is a [rock](https://www.sqlite.org/testing.html)-[solid](https://philipotoole.com/how-is-rqlite-tested/), fault-tolerant, relational database with **effortless installation, deployment, and operation**. You can run a single node for a SQLite-over-the-network solution, or scale up to a cluster for high-availabilty. rqlite is ideal as a lightweight, distributed relational data store for both developers and operators. Think [Consul](https://www.consul.io/) or [etcd](https://etcd.io/), but with relational modeling available.

**Use rqlite to reliably store your most important data, ensuring it's always available to your applications**. If you're interested in understanding how distributed systems actually work, it's a good example to study. A lot of thought has gone into its [design](https://rqlite.io/docs/design/), separating storage, consensus, and API clearly.

### Key features
- **Easy Deployment**: Up and running in seconds, with no separate SQLite installation.
- **Developer-Friendly**: Easy-to-use [HTTP API](https://rqlite.io/docs/api/), [CLI](https://rqlite.io/docs/cli/) and [client libraries](https://rqlite.io/docs/api/client-libraries/).
- **Rich feature set**: [Full-text search](https://www.sqlite.org/fts5.html), [JSON support](https://www.sqlite.org/json1.html), and [SQLite extensions support](https://rqlite.io/docs/guides/extensions/) including [Vector Search](https://github.com/asg017/sqlite-vec) and [Crypto](https://github.com/nalgeon/sqlean).
- **Large data set support**: rqlite works well, even when managing multi-GB data sets.
- **Reliable**: Fully replicated SQL database provides fault-tolerance and high-availability.
- **Dynamic Clustering**: Integrates with [Kubernetes](https://rqlite.io/docs/guides/kubernetes/), [Docker Compose](https://rqlite.io/docs/guides/docker-compose/), Consul, etcd, and DNS for [automatic clustering](https://rqlite.io/docs/clustering/automatic-clustering/).
- **Robust Security**: [Extensive encryption and TLS support](https://rqlite.io/docs/guides/security/).
- **Flexible Consistency**: Customize [read/write performance](https://rqlite.io/docs/api/read-consistency/) and [durability](https://rqlite.io/docs/api/queued-writes/).
- **Scalable Reads**: [Read-only nodes](https://rqlite.io/docs/clustering/read-only-nodes/) for enhanced scalability.
- **Transactions**: Supports a **form** of transactions.
- **Easy Backups**: Hot [backups](https://rqlite.io/docs/guides/backup/), including [automatic backups to AWS S3 and MinIO](https://rqlite.io/docs/guides/backup/#automatic-backups), as well as [restore directly from SQLite](https://rqlite.io/docs/guides/backup/#restoring-from-sqlite).

## More questions?
- [Join the rqlite Slack channel](https://rqlite.io/join-slack)
- [Sign up for Office Hours](https://rqlite.io/office-hours)


## Pronunciation
Common pronunciations of rqlite include "R Q lite" and "ree-qwell-lite".
