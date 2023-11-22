module github.com/rqlite/rqlite

go 1.16

require (
	github.com/Bowery/prompt v0.0.0-20190916142128-fa8279994f75
	github.com/armon/go-metrics v0.5.2 // indirect
	github.com/aws/aws-sdk-go v1.48.2
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/fatih/color v1.16.0 // indirect
	github.com/hashicorp/consul/api v1.26.1 // indirect
	github.com/hashicorp/go-msgpack v1.1.5 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/hashicorp/raft v1.6.0
	github.com/labstack/gommon v0.4.1 // indirect
	github.com/mkideal/cli v0.2.7
	github.com/mkideal/pkg v0.1.3
	github.com/rqlite/go-sqlite3 v1.29.0
	github.com/rqlite/raft-boltdb/v2 v2.0.0-20230523104317-c08e70f4de48
	github.com/rqlite/rqlite-disco-clients v0.0.0-20231121120431-b2b3f3f258b8
	github.com/rqlite/sql v0.0.0-20221103124402-8f9ff0ceb8f0
	go.etcd.io/bbolt v1.3.8
	go.etcd.io/etcd/client/v3 v3.5.10 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.26.0 // indirect
	golang.org/x/crypto v0.15.0
	golang.org/x/exp v0.0.0-20231110203233-9a3e6036ecaa // indirect
	golang.org/x/net v0.18.0
	google.golang.org/genproto v0.0.0-20231120223509-83a465c0220f // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20231120223509-83a465c0220f // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20231120223509-83a465c0220f // indirect
	google.golang.org/protobuf v1.31.0
)

replace (
	github.com/armon/go-metrics => github.com/hashicorp/go-metrics v0.5.1
	golang.org/x/text => golang.org/x/text v0.3.8
)
