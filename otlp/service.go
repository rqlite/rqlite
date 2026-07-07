// Package otlp provides OpenTelemetry metrics reporting for rqlite. It
// bridges rqlite's expvar metrics to the OpenTelemetry metrics data model,
// and periodically pushes them, along with Go runtime metrics, in OTLP
// format over gRPC to an OpenTelemetry Collector. The expvar metrics
// themselves are not modified in any way by this package.
package otlp

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/rqlite/rqlite/v10/internal/rsync"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"google.golang.org/grpc/credentials"
)

const stopTimeout = 5 * time.Second

// Service periodically pushes rqlite metrics, in OTLP format, to an
// OpenTelemetry Collector.
type Service struct {
	cfg     Config
	mp      *sdkmetric.MeterProvider
	running *rsync.AtomicBool

	logger *log.Logger
}

// NewService returns a new Service with the given configuration. Reporting
// does not begin until Start is called.
func NewService(cfg Config) *Service {
	return &Service{
		cfg:     cfg,
		running: rsync.NewAtomicBool(),
		logger:  log.New(os.Stderr, "[otlp-metrics] ", log.LstdFlags),
	}
}

// Start starts periodic reporting of metrics to the Collector. The
// connection to the Collector is established lazily, so Start succeeds
// even if the Collector is not yet reachable.
func (s *Service) Start() error {
	if s.running.Is() {
		return nil
	}
	if err := s.cfg.Validate(); err != nil {
		return err
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("rqlite"),
			semconv.ServiceVersion(s.cfg.Version),
			semconv.ServiceInstanceID(s.cfg.NodeID),
		),
	)
	if err != nil {
		return fmt.Errorf("failed to create resource: %s", err)
	}

	opts := []otlpmetricgrpc.Option{
		otlpmetricgrpc.WithEndpoint(s.cfg.Endpoint),
	}
	if s.cfg.Insecure {
		opts = append(opts, otlpmetricgrpc.WithInsecure())
	} else {
		tlsCfg, err := s.cfg.TLSConfig()
		if err != nil {
			return fmt.Errorf("failed to create TLS config: %s", err)
		}
		opts = append(opts, otlpmetricgrpc.WithTLSCredentials(credentials.NewTLS(tlsCfg)))
	}
	exp, err := otlpmetricgrpc.New(context.Background(), opts...)
	if err != nil {
		return fmt.Errorf("failed to create OTLP exporter: %s", err)
	}

	s.mp = sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp,
			sdkmetric.WithInterval(s.cfg.Interval),
			sdkmetric.WithProducer(NewBridge()))),
	)

	// Export happens on a background goroutine, so route any errors it
	// encounters to this service's logger.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		s.logger.Printf("error exporting metrics: %s", err)
	}))

	if err := runtime.Start(runtime.WithMeterProvider(s.mp)); err != nil {
		s.shutdownProvider()
		return fmt.Errorf("failed to start Go runtime metrics collection: %s", err)
	}

	s.running.Set()
	s.logger.Printf("reporting metrics to %s every %s", s.cfg.Endpoint, s.cfg.Interval)
	return nil
}

// Stop stops the service, performing a final export of metrics to the
// Collector before returning.
func (s *Service) Stop() {
	if s.running.IsNot() {
		return
	}
	s.shutdownProvider()
	s.running.Unset()
}

// Stats returns the status of the Service.
func (s *Service) Stats() (map[string]any, error) {
	return map[string]any{
		"endpoint":  s.cfg.Endpoint,
		"interval":  s.cfg.Interval.String(),
		"insecure":  s.cfg.Insecure,
		"no_verify": s.cfg.InsecureSkipVerify,
		"running":   s.running.Is(),
	}, nil
}

func (s *Service) shutdownProvider() {
	ctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	defer cancel()
	if err := s.mp.Shutdown(ctx); err != nil {
		s.logger.Printf("error shutting down metrics provider: %s", err)
	}
}
