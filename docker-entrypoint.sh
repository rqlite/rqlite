#!/bin/bash

# User wants to override options, so merge with defaults.
if [ "${1:0:1}" = '-' ]; then
        set -- rqlited -http-addr 0.0.0.0:4001 -raft-addr 0.0.0.0:4002 $@ /rqlite/file/data
fi

exec "$@"
