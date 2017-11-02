#!/bin/bash

TMP_DATA=`mktemp`

rm $GOPATH/bin/*
go install ./...

openssl req -x509 -nodes -newkey rsa:4096 -keyout ${TMP_DATA}_key.pem -out ${TMP_DATA}_cert.pem -days 365

$GOPATH/bin/rqlited -node-id node0 -http-addr localhost:4001 -raft-addr localhost:4002 -node-cert ${TMP_DATA}_cert.pem -node-key ${TMP_DATA}_key.pem -node-no-verify -node-encrypt ${TMP_DATA}_1 &
sleep 5
$GOPATH/bin/rqlited -node-id node1 -http-addr localhost:4003 -raft-addr localhost:4004 -join http://localhost:4001 -node-cert ${TMP_DATA}_cert.pem -node-key ${TMP_DATA}_key.pem -node-no-verify -node-encrypt ${TMP_DATA}_2 &
sleep 5
$GOPATH/bin/rqlited -node-id node2 -http-addr localhost:4005 -raft-addr localhost:4006 -join http://localhost:4001 -node-cert ${TMP_DATA}_cert.pem -node-key ${TMP_DATA}_key.pem -node-no-verify -node-encrypt ${TMP_DATA}_3 &
sleep 5

wait
