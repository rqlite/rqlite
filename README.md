<img src="DOC/logo-text.png" height=100></img>

[![Circle CI](https://circleci.com/gh/rqlite/rqlite/tree/master.svg?style=svg)](https://circleci.com/gh/rqlite/rqlite/tree/master)
[![appveyor](https://ci.appveyor.com/api/projects/status/github/rqlite/rqlite?branch=master&svg=true)](https://ci.appveyor.com/project/otoolep/rqlite)
[![Go Report Card](https://goreportcard.com/badge/github.com/rqlite/rqlite)](https://goreportcard.com/report/github.com/rqlite/rqlite)
[![Release](https://img.shields.io/github/release/rqlite/rqlite.svg)](https://github.com/rqlite/rqlite/releases)
[![Docker](https://img.shields.io/docker/pulls/rqlite/rqlite?style=plastic)](https://hub.docker.com/r/rqlite/rqlite/)
[![Slack](https://img.shields.io/badge/Slack--purple.svg)](https://www.philipotoole.com/join-rqlite-slack)
[![Google Group](https://img.shields.io/badge/Google%20Group--blue.svg)](https://groups.google.com/group/rqlite)

*rqlite* is an easy-to-use, lightweight, distributed relational database, which uses [SQLite](https://www.sqlite.org/) as its storage engine.

- [Website](https://www.rqlite.io)
- [Developer guide](https://www.rqlite.io/docs/api)

rqlite is simple to deploy, operating and accessing it is very straightforward, and its clustering capabilities provide you with fault-tolerance and high-availability. [rqlite is available for Linux, macOS, and Microsoft Windows](https://github.com/rqlite/rqlite/releases), and can be built for many target CPUs, including x86, AMD, MIPS, RISC, PowerPC, and ARM.

_Check out the [rqlite FAQ](https://rqlite.io/docs/faq)_.

## Why run rqlite?
rqlite gives you the functionality of a [rock solid](https://www.sqlite.org/testing.html), fault-tolerant, replicated relational database, but with very **easy installation, deployment, and operation**. With it you've got a **lightweight** and **reliable distributed relational data store**.

You could use rqlite as part of a larger system, as a central store for some critical relational data, without having to run larger, more complex distributed databases.

And if you're interested in understanding how distributed systems actually work, **rqlite is a good example to study**. Much thought has gone into its [design](https://rqlite.io/docs/design/) and implementation, with clear separation between the various components, including storage, distributed consensus, and API.

### Key features
- Trivially easy to deploy, with no need to separately install SQLite.
- Super-simple to use, with a straightforward [HTTP API](https://rqlite.io/docs/api/). A [command-line interface is also available](https://rqlite.io/docs/cli/), as are various [client libraries](https://github.com/rqlite).
- Fully replicated production-grade SQL database, with full access to SQLite [full-text search](https://www.sqlite.org/fts3.html) and [JSON document support](https://www.sqlite.org/json1.html).
- Multiple options for [node-discovery and automatic clustering, including integration with Kubernetes, Consul, etcd and DNS](https://rqlite.io/docs/clustering/automatic-clustering/), allowing clusters to be dynamically created.
- [Extensive security and encryption support](https://rqlite.io/docs/guides/security/), including support for node-to-node encryption and mutual TLS.
- Choice of [read consistency levels](https://rqlite.io/docs/api/read-consistency/), and support for choosing [write performance over durability](https://rqlite.io/docs/api/queued-writes/).
- Optional [read-only (non-voting) nodes](https://rqlite.io/docs/clustering/read-only-nodes/), which can add read scalability to the system.
- A form of transaction support.
- Hot [backups](https://rqlite.io/docs/guides/backup/), including [automatic backups to AWS S3](https://rqlite.io/docs/guides/backup/#automatic-backups), as well as [restore directly from SQLite](https://rqlite.io/docs/guides/restore/) and AWS S3.

## Quick Start
Check out the [_Quick Start_ guide](https://rqlite.io/docs/quick-start/).

## Pronunciation?
How do I pronounce rqlite? For what it's worth I try to pronounce it "ree-qwell-lite". But it seems most people, including me, often pronounce it "R Q lite".
