package http

import (
	"net/http"
	"strings"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
)

// tracedRoutes are the API routes for which spans are created, with match
// semantics mirroring the dispatch in serveHTTP. Probe, UI, and debug
// endpoints are deliberately excluded, as spans of those requests have
// little diagnostic value.
var tracedRoutes = []struct {
	route string
	exact bool
}{
	{"/db/execute", false},
	{"/db/query", false},
	{"/db/request", false},
	{"/db/backup", false},
	{"/db/load", false},
	{"/db/sql", false},
	{"/boot", true},
	{"/snapshot", true},
	{"/reap", true},
	{"/remove", false},
}

// traceRoute returns the route to use as the span name for the given
// request path, and whether the path should be traced at all.
func traceRoute(path string) (string, bool) {
	for _, tr := range tracedRoutes {
		if tr.exact {
			if path == tr.route {
				return tr.route, true
			}
		} else if strings.HasPrefix(path, tr.route) {
			return tr.route, true
		}
	}
	return "", false
}

// ServeHTTP allows Service to serve HTTP requests. If tracing is enabled,
// requests to API routes are wrapped in a server span, and any W3C trace
// context on the request is honored.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.tracer == nil {
		s.serveHTTP(w, r)
		return
	}
	route, ok := traceRoute(r.URL.Path)
	if !ok {
		s.serveHTTP(w, r)
		return
	}

	ctx := s.propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	ctx, span := s.tracer.Start(ctx, r.Method+" "+route,
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			semconv.HTTPRequestMethodKey.String(r.Method),
			semconv.HTTPRoute(route),
			semconv.URLPath(r.URL.Path),
		))
	defer span.End()

	sw := &statusResponseWriter{ResponseWriter: w}
	s.serveHTTP(sw, r.WithContext(ctx))

	code := sw.statusCode()
	span.SetAttributes(semconv.HTTPResponseStatusCode(code))
	if code >= 500 {
		span.SetStatus(codes.Error, http.StatusText(code))
	}
}

// statusResponseWriter wraps an http.ResponseWriter, recording the status
// code of the response.
type statusResponseWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusResponseWriter) WriteHeader(code int) {
	if w.code == 0 {
		w.code = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusResponseWriter) Write(b []byte) (int, error) {
	if w.code == 0 {
		w.code = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

// Flush implements http.Flusher if the underlying ResponseWriter does.
func (w *statusResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap supports http.ResponseController.
func (w *statusResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// statusCode returns the recorded status code, defaulting to 200 as
// net/http does when a handler never calls WriteHeader.
func (w *statusResponseWriter) statusCode() int {
	if w.code == 0 {
		return http.StatusOK
	}
	return w.code
}
