# Securing rqlite
rqlite can be secured in various way, and with different levels of control.

## File system security
You should control access to the data directory that each rqlite node uses. There is no reason for any user to directly access this directory.

You are also responsible for securing access to the SQLite database files if you enable "on disk" mode (which is not the default mode). There is no reason for any user to directly access any SQLite file, and doing so may cause rqlite to work incorrectly. If you don't need to access a SQLite database file, then don't enable "on disk" mode. This will maximize file-level security.

## Network security
Each rqlite node listens on 2 TCP ports -- one for the HTTP API, and the other for intra-cluster communications. Only the API port need be reachable from outside the cluster.

So, if possible, configure the network such that the Raft port on each node is only accessible from other nodes in the cluster. There is no need for the Raft port to be accessible by rqlite clients.

If the IP addresses (or subnets) of rqlite clients is also known, it may also be possible to limit access to the HTTP API from those addresses only.

AWS EC2 [Security Groups](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html), for example, support all this functionality. So if running rqlite in the AWS EC2 cloud you can implement this level of security at the network level.

## HTTPS API
rqlite supports HTTPS access, ensuring that all communication between clients and a cluster is encrypted. One way to generate the necessary resources is via [openssl](https://www.openssl.org/):
```
openssl req -x509 -nodes -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365
```
Note that unless you sign the certificate using a trusted authority, you will need to pass `-http-no-verify` to `rqlited`.

## Node-to-node encryption
rqlite supports encryption of all inter-node traffic. To enable this, pass `-node-encrypt` to `rqlited`. Each node must also be supplied with the relevant SSL certificate and corresponding private key, in X.509 format. Note that every node in a cluster must operate with encryption enabled, or none at all.

You can generate private keys and associated certificates in a similar manner as described in the _HTTP API_ section.

## Basic Auth
The HTTP API supports [Basic Auth](https://tools.ietf.org/html/rfc2617). Each rqlite node can be passed a JSON-formatted configuration file, which configures valid usernames and associated passwords for that node. The password string can be in cleartext or [bcrypt hashed](https://en.wikipedia.org/wiki/Bcrypt).

Since the configuration file only controls the node local to it, it's important to ensure the configuration is correct on each node.

### User-level permissions
rqlite, via the configuration file, also supports user-level permissions. Each user can be granted one or more of the following permissions:
- _all_: user can perform all operations on a node.
- _execute_: user may access the execute endpoint.
- _query_: user may access the query endpoint.
- _load_: user may load an SQLite dump file into a node.
- _backup_: user may perform backups.
- _status_: user can retrieve status and Go runtime information.
- _join_: user can join a cluster. In practice only a node joins a cluster, so it's the joining node that must supply the credentials.
- _remove_: user can remove a node from a cluster.

### Example configuration file
An example configuration file is shown below.
```json
[
  {
    "username": "bob",
    "password": "secret1",
    "perms": ["all"]
  },
  {
    "username": "mary",
    "password": "$2a$10$fKRHxrEuyDTP6tXIiDycr.nyC8Q7UMIfc31YMyXHDLgRDyhLK3VFS",
    "perms": ["query", "status"]
  }
]
```
This configuration file sets authentication for two usernames, _bob_ and _mary_, and it sets a password for each. No other users will be able to access the cluster.

This configuration also sets permissions for both users. _bob_ has permission to perform all operations, but _mary_ can only query the cluster, as well as check the cluster status.

## Secure cluster example
Starting a node with HTTPS enabled, node-to-node encryption, and with the above configuration file. It is assumed the HTTPS X.509 certificate and key are at the paths `server.crt` and `key.pem` respectively, and the node-to-node certificate and key are at `node.crt` and `node-key.pem`
```bash
rqlited -auth config.json -http-cert server.crt -http-key key.pem \
-node-encrypt -node-cert node.crt -node-key node-key.pem -node-no-verify \
~/node.1
```
Bringing up a second node on the same host, joining it to the first node. This allows you to block nodes from joining a cluster, unless those nodes supply a password.
```bash
rqlited -auth config.json -http-addr localhost:4003 -http-cert server.crt \
-http-key key.pem -raft-addr :4004 -join https://bob:secret1@localhost:4001 \
-node-encrypt -node-cert node.crt -node-key node-key.pem -no-node-verify \
~/node.2
```
Querying the node, as user _mary_.
```bash
curl -G 'https://mary:secret2@localhost:4001/db/query?pretty&timings' \
--data-urlencode 'q=SELECT * FROM foo'
```
