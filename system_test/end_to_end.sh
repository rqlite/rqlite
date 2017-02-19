#!/bin/bash

TMP_DATA=`mktemp`

rm $GOPATH/bin/*
go install ./...

$GOPATH/bin/rqlited ${TMP_DATA}_1 &
sleep 5
$GOPATH/bin/rqlited -http localhost:4003 -raft localhost:4004 -join http://localhost:4001 ${TMP_DATA}_2 &
sleep 5
$GOPATH/bin/rqlited -http localhost:4005 -raft localhost:4006 -join http://localhost:4001 ${TMP_DATA}_3 &
sleep 5

wait
