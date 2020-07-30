package http

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	pNumExecutions = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "rqlite",
		Subsystem: "server",
		Name:      "executions_total",
		Help:      "The total number of execution requests",
	})
	pNumQueries = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "rqlite",
		Subsystem: "server",
		Name:      "queries_total",
		Help:      "The total number of query requests",
	})
	pNumBackups = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "rqlite",
		Subsystem: "server",
		Name:      "backups_total",
		Help:      "The total number of backup requests",
	})
	pNumLoads = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "rqlite",
		Subsystem: "server",
		Name:      "loads_total",
		Help:      "The total number of load requests",
	})
)

func init() {
	prometheus.MustRegister(pNumExecutions)
	prometheus.MustRegister(pNumQueries)
	prometheus.MustRegister(pNumBackups)
	prometheus.MustRegister(pNumLoads)

}
