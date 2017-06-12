#!/bin/bash

TMP_DATA=`mktemp`

rm $GOPATH/bin/*
go install ./...

openssl req -x509 -nodes -newkey rsa:4096 -keyout ${TMP_DATA}_key.pem -out ${TMP_DATA}_cert.pem -days 365

$GOPATH/bin/rqlited -http localhost:4001 -raft localhost:4002 -nodex509cert ${TMP_DATA}_cert.pem -nodex509key ${TMP_DATA}_key.pem -nonodeverify -encrypt ${TMP_DATA}_1 &
sleep 5
$GOPATH/bin/rqlited -http localhost:4003 -raft localhost:4004 -join http://localhost:4001 -nodex509cert ${TMP_DATA}_cert.pem -nodex509key ${TMP_DATA}_key.pem -nonodeverify -encrypt ${TMP_DATA}_2 &
sleep 5
$GOPATH/bin/rqlited -http localhost:4005 -raft localhost:4006 -join http://localhost:4001 -nodex509cert ${TMP_DATA}_cert.pem -nodex509key ${TMP_DATA}_key.pem -nonodeverify -encrypt ${TMP_DATA}_3 &
sleep 5

wait
