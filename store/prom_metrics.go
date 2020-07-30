package store

import (
	"github.com/prometheus/client_golang/prometheus"
	rprom "github.com/rqlite/rqlite/prometheus"
)

func registerFileSizeMetric(fqName, help, path string) {
	fm := rprom.NewFileSizeCollector(fqName, help, path)
	prometheus.MustRegister(fm)
}

var (
	pNumSnaphots = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "rqlite",
		Subsystem: "server",
		Name:      "raft_snapshots_total",
		Help:      "The total number of Raft snapshots",
	})
	pNumBackups = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "rqlite",
		Subsystem: "server",
		Name:      "store_backups_total",
		Help:      "The total number of database backups",
	})
	pNumRestores = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "rqlite",
		Subsystem: "server",
		Name:      "raft_restores_total",
		Help:      "The total number of Raft restores",
	})
)

func init() {
	prometheus.MustRegister(pNumSnaphots)
	prometheus.MustRegister(pNumBackups)
	prometheus.MustRegister(pNumRestores)
}
