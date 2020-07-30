package prometheus

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
)

type FileSizeCollector struct {
	fqName string
	help   string
	path   string
}

func NewFileSizeCollector(fqName, help, path string) *FileSizeCollector {
	return &FileSizeCollector{
		fqName: fqName,
		help:   help,
		path:   path,
	}
}

func (f FileSizeCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(f, ch)
}

func (f FileSizeCollector) Collect(ch chan<- prometheus.Metric) {
	desc := prometheus.NewDesc(
		f.fqName,
		f.help,
		nil,
		nil,
	)

	stat, err := os.Stat(f.path)
	if err == nil {
		ch <- prometheus.MustNewConstMetric(
			desc,
			prometheus.GaugeValue,
			float64(stat.Size()),
		)
	} else {
		ch <- prometheus.MustNewConstMetric(
			desc,
			prometheus.GaugeValue,
			float64(0),
		)
	}
}
