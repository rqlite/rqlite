#!/bin/bash

CACONF=`mktemp`
TMPDIR=`mktemp -d`

# Generate a private key for the CA
openssl genrsa -out $TMPDIR/root.key.pem 4096

# Generate cert from the root key
# You can review the cert afterwards via `openssl x509 -in root.cert.pem -text`
openssl req -key $TMPDIR/root.key.pem -new -x509 -days 7300 -sha256 -extensions v3_ca -out $TMPDIR/root.cert.pem  -subj "/C=US/ST=Pennsylvania/L=Mars/O=rqlite /OU=IT Department/CN=ca.rqlite.io"

# Generate key for a server, and a corresponding certificate signing request (CSR).
# Review the CSR via `openssl req -in server.csr.pem -text -noout`
openssl req -new -newkey rsa:1024 -nodes -keyout $TMPDIR/server.key.pem -out $TMPDIR/server.csr.pem -subj "/C=US/ST=Pennsylvania/L=Mars/O=rqlite /OU=IT Department/CN=server.rqlite.io"

# Generate key for a client, and a corresponding certificate signing request (CSR).
# Review the CSR via `openssl req -in client.csr.pem -text -noout`
openssl req -new -newkey rsa:1024 -nodes -keyout $TMPDIR/client.key.pem -out $TMPDIR/client.csr.pem -subj "/C=US/ST=Pennsylvania/L=Mars/O=rqlite /OU=IT Department/CN=client.rqlite.io"

# Prep the "certificate authority" to process the CSR.
echo "[ ca ]
default_ca = ca_default
[ ca_default ]
certs = $TMPDIR
new_certs_dir = $TMPDIR/ca.db.certs
database = $TMPDIR/ca.db.index
serial = $TMPDIR/ca.db.serial
RANDFILE = $TMPDIR/ca.db.rand
certificate = $TMPDIR/root.cert.pem
private_key = $TMPDIR/root.key.pem
default_days = 7300
default_crl_days = 30
default_md = md5
preserve = no
policy = generic_policy
[ generic_policy ]
countryName = optional
stateOrProvinceName = optional
localityName = optional
organizationName = optional
organizationalUnitName = optional
commonName = optional
emailAddress = optional
">$CACONF

mkdir $TMPDIR/ca.db.certs
touch $TMPDIR/ca.db.index
echo "1234" > $TMPDIR/ca.db.serial

# Process the CSR
openssl ca -config $CACONF -batch -out $TMPDIR/client.cert.pem -infiles $TMPDIR/client.csr.pem

echo "CA root private key: $TMPDIR/root.key.pem"
echo "CA root public cert: $TMPDIR/root.cert.pem"
echo "Server private key: $TMPDIR/server.key.pem"
echo "Server cert signed by root CA: $TMPDIR/server.cert.pem"
echo "Client private key: $TMPDIR/client.key.pem"
echo "Client cert signed by root CA: $TMPDIR/client.cert.pem"

