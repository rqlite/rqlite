#!/bin/sh

contains() {
	key=$1
	shift
	for i in "$@"
	do
		if [[ $i == $key* ]]; then
			return 1
		fi
	done
	return 0
}

DEFAULT_NODE_ID=`hostname`
DEFAULT_ADV_ADDRESS=`hostname -f`

contains "-http-addr" "$@"
if [ $? -eq 0 ]; then
	if [ -z "$HTTP_ADDR" ]; then
		HTTP_ADDR="0.0.0.0:4001"
	fi
	http_addr="-http-addr $HTTP_ADDR"
fi

contains "-http-adv-addr" "$@"
if [ $? -eq 0 ]; then
	if [ -z "$HTTP_ADV_ADDR" ]; then
		HTTP_ADV_ADDR=$DEFAULT_ADV_ADDRESS:4001
	fi
	http_adv_addr="-http-adv-addr $HTTP_ADV_ADDR"
fi

contains "-raft-adv" "$@"
if [ $? -eq 0 ]; then
	if [ -z "$RAFT_ADDR" ]; then
		RAFT_ADDR="0.0.0.0:4002"
	fi
	raft_addr="-raft-addr $RAFT_ADDR"
fi

contains "-raft-adv-addr" "$@"
if [ $? -eq 0 ]; then
	if [ -z "$RAFT_ADV_ADDR" ]; then
		RAFT_ADV_ADDR=$DEFAULT_ADV_ADDRESS:4002
	fi
	raft_adv_addr="-raft-adv-addr $RAFT_ADV_ADDR"
fi

contains "-node-id" "$@"
if [ $? -eq 0 ]; then
	if [ -z "$NODE_ID" ]; then
		NODE_ID="$DEFAULT_NODE_ID"
	fi
	node_id="-node-id $NODE_ID"
fi

if [ -z "$DATA_DIR" ]; then
	DATA_DIR="/rqlite/file/data"
fi

contains "-extensions_path" "$@"
if [ $? -eq 0 ]; then
	extensions_path=""
	if [ -n "$SQLITE_EXTENSIONS" ]; then
		for ext in $SQLITE_EXTENSIONS; do
			if [ -z "$extensions_path" ]; then
				extensions_path="/opt/extensions/$ext"
			else
				extensions_path="${extensions_path},/opt/extensions/$ext"
			fi
		done
	fi
	if [ -n "$CUSTOM_SQLITE_EXTENSION_PATHS" ]; then
		if [ -z "$extensions_path" ]; then
			extensions_path=$CUSTOM_SQLITE_EXTENSION_PATHS
		else
			extensions_path="${extensions_path},$CUSTOM_SQLITE_EXTENSION_PATHS"
		fi
	fi
	if [ -n "$extensions_path" ]; then
		extensions_path_flag="-extensions-path=$extensions_path"
	fi
fi

# When running on Kubernetes, delay a small time so DNS records
# are configured across the cluster when this rqlited comes up. Because
# rqlite does node-discovery using a headless service, it must have
# accurate DNS records. If the Pods addresses are not in the records,
# the DNS lookup will result in an error, and the Kubernetes system will
# cache this failure for (by default) 30 seconds. So this delay
# actually means getting to "ready" is quicker.
#
# This is kind of a hack. If anyone knows a better way file
# a GitHub issue at https://github.com/rqlite/rqlite.
if [ -n "$KUBERNETES_SERVICE_HOST" ]; then
      if [ -z "$START_DELAY" ]; then
            START_DELAY=5
      fi
fi
if [ -n "$START_DELAY" ]; then
      sleep "$START_DELAY"
fi

RQLITED=/bin/rqlited
rqlited_commands="$RQLITED $node_id $http_addr $http_adv_addr $raft_addr $raft_adv_addr $extensions_path_flag"
data_dir="$DATA_DIR"

if [ "$1" = "rqlite" ]; then
        set -- $rqlited_commands $data_dir
elif [ "${1:0:1}" = '-' ]; then
        # User is passing some options, so merge them.
        set -- $rqlited_commands $@ $data_dir
fi

exec "$@"
