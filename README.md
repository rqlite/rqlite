<img src="DOC/logo-text.png" height=100></img>

[![Circle CI](https://circleci.com/gh/rqlite/rqlite/tree/master.svg?style=svg)](https://circleci.com/gh/rqlite/rqlite/tree/master)
[![appveyor](https://ci.appveyor.com/api/projects/status/github/rqlite/rqlite?branch=master&svg=true)](https://ci.appveyor.com/project/otoolep/rqlite)
[![Go Report Card](https://goreportcard.com/badge/github.com/rqlite/rqlite)](https://goreportcard.com/report/github.com/rqlite/rqlite/v8)
[![Release](https://img.shields.io/github/release/rqlite/rqlite.svg)](https://github.com/rqlite/rqlite/releases)
[![Docker](https://img.shields.io/docker/pulls/rqlite/rqlite?style=plastic)](https://hub.docker.com/r/rqlite/rqlite/)
[![Slack](https://img.shields.io/badge/Slack--purple.svg)](https://www.philipotoole.com/join-rqlite-slack)
[![Google Group](https://img.shields.io/badge/Google%20Group--blue.svg)](https://groups.google.com/group/rqlite)

*rqlite* is a relational database which combines SQLite's simplicity with the power of a robust, fault-tolerant, distributed system. It's designed for easy deployment and lightweight operation, offering a developer-friendly and operator-centric solution for [Linux, macOS, and Windows, as well as various CPU platforms](https://github.com/rqlite/rqlite/releases).

- [Website](https://www.rqlite.io)
- [Developer guide](https://www.rqlite.io/docs/api)

_Check out the [rqlite FAQ](https://rqlite.io/docs/faq)_.

## Why run rqlite?
rqlite is your solution for a [rock-solid](https://www.sqlite.org/testing.html), fault-tolerant, relational database with **effortless installation, deployment, and operation**. It's ideal as a lightweight, distributed relational data store for both developers and operators. Think [Consul](https://www.consul.io/) or [etcd](https://etcd.io/), but with relational modeling available.

You can use rqlite to store your important data reliably, ensuring it's always available. If you're interested in understanding how distributed systems actually work, it's a good example to study. A lot of thought has gone into its [design](https://rqlite.io/docs/design/), separating storage, consensus, and API clearly.

### Key features
- **Easy Deployment**: Up and running in seconds, with no separate SQLite installation.
- **Developer-Friendly**: Straightforward [HTTP API](https://rqlite.io/docs/api/), [CLI](https://rqlite.io/docs/cli/), and [client libraries](https://rqlite.io/docs/api/client-libraries/).
- **Fully Replicated**: SQL database with [full-text search](https://www.sqlite.org/fts5.html) and [JSON support](https://www.sqlite.org/json1.html).
- **Large data set support**: rqlite works well, even when managing multi-GB data sets.
- **Dynamic Clustering**: Integrates with [Kubernetes](https://rqlite.io/docs/guides/kubernetes/), Consul, etcd, and DNS for [automatic clustering](https://rqlite.io/docs/clustering/automatic-clustering/).
- **Robust Security**: [Extensive encryption and TLS support](https://rqlite.io/docs/guides/security/).
- **Flexible Consistency**: Customize [read/write performance](https://rqlite.io/docs/api/read-consistency/) and [durability](https://rqlite.io/docs/api/queued-writes/).
- **Scalable Reads**: [Read-only nodes](https://rqlite.io/docs/clustering/read-only-nodes/) for enhanced scalability.
- **Transactions**: Supports a form of transactions.
- **Easy Backups**: Hot [backups](https://rqlite.io/docs/guides/backup/), including [automatic backups to AWS S3 and MinIO](https://rqlite.io/docs/guides/backup/#automatic-backups), as well as [restore directly from SQLite](https://rqlite.io/docs/guides/backup/#restoring-from-sqlite).

## Quick Start
Get up and running quickly with our [_Quick Start_ guide](https://rqlite.io/docs/quick-start/).

## Pronunciation
Common pronunciations of rqlite include "R Q lite" and "ree-qwell-lite".
