package interfaces

import "github.com/rcrowley/go-metrics"

// Statistics is an interface for metrics statistics
type Statistics interface {
	GetStatistics() (metrics.Registry, error)
}
