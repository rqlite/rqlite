# Securing rqlite
rqlite can be secured in various way, and at different levels of control.

## File system security
You are responsible for securing access to the SQLite database files if you enable "on disk" mode (which is not the default mode). There is no reason for any user to directly access any SQLite file, and doing so may cause rqlite to work incorrectly.

## Network security
Each rqlite node listens on 2 TCP ports -- one for the HTTP API, and the other for intra-cluster communications. Only the API port need be reachable from outside the cluster.

So, if possible, configure the network such that the Raft port on each node is only accessible from other nodes in the cluster. There is no need for the Raft port to be accessible by rqlite clients.

If the IP addresses (or subnets) of rqlite clients is also known, it may also be possible to limit access to the HTTP API from those addresses only.

AWS EC2 [Security Groups](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html), for example, support all this functionality. So if running rqlite in the AWS EC2 cloud you can implement this level of security at the network level.

## HTTPS API
rqlite supports HTTPS access, ensuring that all communication between clients and a cluster is encrypted.

## Basic Auth
The HTTP API supports [Basic Auth](https://tools.ietf.org/html/rfc2617). Each rqlite node can be passed a JSON-formatted configuration file, which configures valid usernames and associated passwords for that node.

Since the configuration file only controls the node local to it, it's important to ensure the configuration is correct on each node.

### User-level permissions
rqlite, via the configuration file, also supports user-level permissions. Each user can be granted one or more of the following permissions:
- _all_: user can perform all operations on a node.
- _execute_: user may access the execute endpoint.
- _query_: user may access the query endpoint.
- _backup_: user may perform backups.
- _status_: user can retrieve status information from the node.
- _join_: user can join a cluster. In practice only a node joins a cluster.
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
    "password": "secret2",
    "perms": ["query"]
  }
]
```
This configuration file sets authentication for two usernames, _bob_ and _mary_, and it sets a password for each. No other users will be able to access the cluster.

This configuration also sets permissions for both users. _bob_ has permission to perform all operations, and _mary_ can only query the cluster.

## Secure cluster example
Starting a node with HTTPS enabled and with the above configuration file. It is assumed the X.509 certificate and key are at the paths `server.crt` and `key.pem` respectively.
```bash
rqlited -auth config.json -x509cert server.crt -x509key key.pem ~/node.1
```
Bringing up a second node, joining it to the first node.
```bash
rqlited -auth config.json -http localhost:4003 -x509cert server.crt \
-x509key key.pem -raft :4004 -join https://bob:secret1@localhost:4001 \
~/node.2
```
Querying the node, as user _mary_.
```bash
curl -G 'https://mary:secret2@localhost:4001/db/query?pretty&timings' \
--data-urlencode 'q=SELECT * FROM foo'
```
