#!/bin/bash

TMP_DATA=`mktemp`

rm $GOPATH/bin/*
go install ./...

$GOPATH/bin/rqlited -node-id node0 -http-addr localhost:4001 -raft-addr localhost:4002 ${TMP_DATA}_1 &
sleep 5
$GOPATH/bin/rqlited -node-id node1 -http-addr localhost:4003 -raft-addr localhost:4004 -join http://localhost:4001 ${TMP_DATA}_2 &
sleep 5
$GOPATH/bin/rqlited -node-id node2 -http-addr localhost:4005 -raft-addr localhost:4006 -join http://localhost:4001 ${TMP_DATA}_3 &
sleep 5

wait
