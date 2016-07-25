#!/bin/bash

cd cmd/rqlited
rm -f rqlited
go build .
cp rqlited $CIRCLE_ARTIFACTS
