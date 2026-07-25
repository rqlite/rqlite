// Package otlp provides OpenTelemetry telemetry reporting for rqlite. It
// bridges rqlite's expvar metrics to the OpenTelemetry metrics data model,
// and periodically pushes them, along with Go runtime metrics, in OTLP
// format over gRPC to an OpenTelemetry Collector. The expvar metrics
// themselves are not modified in any way by this package. It also exports
// trace spans, created via the Service's TracerProvider, to the same
// Collector.
package otlp

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/rqlite/rqlite/v10/internal/rsync"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/credentials"
)

const stopTimeout = 5 * time.Second

// Service pushes rqlite metrics and traces, in OTLP format, to an
// OpenTelemetry Collector.
type Service struct {
	cfg     Config
	mp      *sdkmetric.MeterProvider
	tp      *sdktrace.TracerProvider
	running *rsync.AtomicBool

	logger *log.Logger
}

// NewService returns a new Service with the given configuration. Reporting
// does not begin until Start is called.
func NewService(cfg Config) *Service {
	return &Service{
		cfg:     cfg,
		running: rsync.NewAtomicBool(),
		logger:  log.New(os.Stderr, "[otlp] ", log.LstdFlags),
	}
}

// Start starts reporting of metrics and traces to the Collector. The
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

	var tlsCfg *tls.Config
	if !s.cfg.Insecure {
		tlsCfg, err = s.cfg.TLSConfig()
		if err != nil {
			return fmt.Errorf("failed to create TLS config: %s", err)
		}
	}

	mopts := []otlpmetricgrpc.Option{
		otlpmetricgrpc.WithEndpoint(s.cfg.Endpoint),
		// Disable retry of failed exports. Exported metrics are cumulative,
		// so the next export makes up for any failed one. More importantly,
		// retrying can block the final export at shutdown until stopTimeout
		// expires, needlessly delaying node shutdown when the Collector is
		// unreachable.
		otlpmetricgrpc.WithRetry(otlpmetricgrpc.RetryConfig{Enabled: false}),
	}
	if s.cfg.Insecure {
		mopts = append(mopts, otlpmetricgrpc.WithInsecure())
	} else {
		mopts = append(mopts, otlpmetricgrpc.WithTLSCredentials(credentials.NewTLS(tlsCfg)))
	}
	exp, err := otlpmetricgrpc.New(context.Background(), mopts...)
	if err != nil {
		return fmt.Errorf("failed to create OTLP metric exporter: %s", err)
	}

	s.mp = sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp,
			sdkmetric.WithInterval(s.cfg.Interval),
			sdkmetric.WithProducer(NewBridge()))),
	)

	// Unlike metrics, retry of failed exports is left enabled for traces.
	// A span that fails to export is lost forever. Shutdown remains bounded
	// regardless, since the final flush respects the stopTimeout context.
	topts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(s.cfg.Endpoint),
	}
	if s.cfg.Insecure {
		topts = append(topts, otlptracegrpc.WithInsecure())
	} else {
		topts = append(topts, otlptracegrpc.WithTLSCredentials(credentials.NewTLS(tlsCfg)))
	}
	texp, err := otlptracegrpc.New(context.Background(), topts...)
	if err != nil {
		s.shutdownProviders()
		return fmt.Errorf("failed to create OTLP trace exporter: %s", err)
	}

	s.tp = sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
		sdktrace.WithBatcher(texp),
	)

	// Export happens on a background goroutine, so route any errors it
	// encounters to this service's logger.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		s.logger.Printf("error exporting telemetry: %s", err)
	}))

	if err := runtime.Start(runtime.WithMeterProvider(s.mp)); err != nil {
		s.shutdownProviders()
		return fmt.Errorf("failed to start Go runtime metrics collection: %s", err)
	}

	s.running.Set()
	s.logger.Printf("reporting metrics every %s, and traces as they complete, to %s",
		s.cfg.Interval, s.cfg.Endpoint)
	return nil
}

// Stop stops the service, performing a final export of metrics and any
// remaining spans to the Collector before returning.
func (s *Service) Stop() {
	if s.running.IsNot() {
		return
	}
	s.shutdownProviders()
	s.running.Unset()
}

// TracerProvider returns the provider to use for creating trace spans.
// It returns nil unless the Service has been started.
func (s *Service) TracerProvider() trace.TracerProvider {
	if s.tp == nil {
		return nil
	}
	return s.tp
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

func (s *Service) shutdownProviders() {
	ctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	defer cancel()
	if s.tp != nil {
		if err := s.tp.Shutdown(ctx); err != nil {
			s.logger.Printf("error shutting down trace provider: %s", err)
		}
	}
	if s.mp != nil {
		if err := s.mp.Shutdown(ctx); err != nil {
			s.logger.Printf("error shutting down metrics provider: %s", err)
		}
	}
}
