#!/bin/sh

contains() {
	key=$1
	shift
	for a in "$@"; do
		case "$a" in
			"$key"*) return 1 ;;
		esac
	done
	return 0
}

DEFAULT_NODE_ID=`hostname`
DEFAULT_ADV_ADDRESS=`hostname -f`

CMD=$1
ARGS="$@"

contains "-http-addr" $ARGS
if [ $? -eq 0 ]; then
	if [ -z "$HTTP_ADDR" ]; then
		HTTP_ADDR="0.0.0.0:4001"
	fi
	http_addr="-http-addr $HTTP_ADDR"
fi

contains "-http-adv-addr" $ARGS
if [ $? -eq 0 ]; then
	if [ -z "$HTTP_ADV_ADDR" ]; then
		HTTP_ADV_ADDR=$DEFAULT_ADV_ADDRESS:4001
	fi
	http_adv_addr="-http-adv-addr $HTTP_ADV_ADDR"
fi

contains "-raft-adv" $ARGS
if [ $? -eq 0 ]; then
	if [ -z "$RAFT_ADDR" ]; then
		RAFT_ADDR="0.0.0.0:4002"
	fi
	raft_addr="-raft-addr $RAFT_ADDR"
fi

contains "-raft-adv-addr" $ARGS
if [ $? -eq 0 ]; then
	if [ -z "$RAFT_ADV_ADDR" ]; then
		RAFT_ADV_ADDR=$DEFAULT_ADV_ADDRESS:4002
	fi
	raft_adv_addr="-raft-adv-addr $RAFT_ADV_ADDR"
fi

contains "-node-id" $ARGS
if [ $? -eq 0 ]; then
	if [ -z "$NODE_ID" ]; then
		NODE_ID="$DEFAULT_NODE_ID"
	fi
	node_id="-node-id $NODE_ID"
fi

extensions_path=""
if [ -n "$SQLITE_EXTENSIONS" ]; then
	contains "-extensions-path" $ARGS
	if [ $? -eq 1 ]; then
		echo "Setting both -extensions-path and SQLITE_EXTENSIONS is not allowed"
  		exit 1
	fi

	case "$SQLITE_EXTENSIONS" in
		*,*)
			IFS=","
			;;
	esac
	for ext in $SQLITE_EXTENSIONS; do
		if [ -z "$extensions_path" ]; then
			extensions_path="/opt/extensions/$ext"
		else
			extensions_path="${extensions_path},/opt/extensions/$ext"
		fi
	done
	unset IFS
fi

if [ -n "$extensions_path" ]; then
	extensions_path_flag="-extensions-path=$extensions_path"
fi

if [ -n "$CUSTOM_SQLITE_EXTENSIONS_PATH" ]; then
	contains "-extensions-path" $ARGS
	if [ $? -eq 1 ]; then
		echo "Setting both -extensions-path and CUSTOM_SQLITE_EXTENSIONS_PATH is not allowed"
  		exit 1
	fi

	if [ -n "$extensions_path_flag" ]; then
		extensions_path_flag="$extensions_path_flag,$CUSTOM_SQLITE_EXTENSIONS_PATH"
	else
		extensions_path_flag="-extensions-path=$CUSTOM_SQLITE_EXTENSIONS_PATH"
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

if [ -z "$DATA_DIR" ]; then
	DATA_DIR="/rqlite/file/data"
fi
data_dir="$DATA_DIR"

if [ "$CMD" = "rqlited" ]; then
        set -- $rqlited_commands $data_dir
elif [ "$(expr substr "$CMD" 1 1)" = "-" ]; then
        # User is passing some options, so merge them.
        set -- $rqlited_commands $@ $data_dir
fi

exec "$@"
