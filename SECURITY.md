# Security

## Hardening your rqlite deployment
To maximize the security of your cluster you should work through the [rqlite _Security guide_](https://rqlite.io/docs/guides/security/). This guide tells you how to harden an rqlite deployment. Work through the sections on that page and apply each one that is relevant to your environment.

## Reporting Vulnerabilities
You can report Vulnerabilities via the GitHub [Security page](https://github.com/rqlite/rqlite/security). 

### What is considered a vulnerability in rqlite
rqlite optimizes for low-friction local development out of the box and provides comprehensive authentication, TLS/mTLS, and network-isolation controls for production deployments. Issues that require running rqlite contrary to the Security Guide are considered operator misconfigurations, not vulnerabilities.

Specifically, reports demonstrating the following may be closed as _Working as Intended_:
- Unauthorized data access, modification, or Denial-of-Service against nodes running with authentication (-auth) disabled.
- Attacks against the Raft consensus port when exposed to untrusted networks without network-level isolation and mutual TLS.
