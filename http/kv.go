package http

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/rqlite/rqlite/v10/auth"
	"github.com/rqlite/rqlite/v10/store"
)

// MaxKVValueBytes caps the size of a value that may be PUT via the KV API.
const MaxKVValueBytes = 8 << 20 // 8 MiB

// kvContentType is the Content-Type set on KV GET responses. Values are
// stored as opaque bytes so we cannot say anything more specific.
const kvContentType = "application/octet-stream"

// handleKV dispatches /kv/<key> requests based on HTTP method.
func (s *Service) handleKV(w http.ResponseWriter, r *http.Request, qp QueryParams) {
	rawKey := strings.TrimPrefix(r.URL.Path, "/kv/")
	key, err := url.PathUnescape(rawKey)
	if err != nil {
		http.Error(w, "invalid key encoding", http.StatusBadRequest)
		return
	}
	if key == "" {
		http.Error(w, "empty key", http.StatusBadRequest)
		return
	}

	if !s.CheckRequestPerm(r, auth.PermKV) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	switch r.Method {
	case http.MethodGet:
		stats.Add(numKVGets, 1)
		s.kvGet(w, r, qp, key)
	case http.MethodPut:
		stats.Add(numKVPuts, 1)
		s.kvPut(w, r, qp, key)
	case http.MethodDelete:
		stats.Add(numKVDeletes, 1)
		s.kvDelete(w, r, qp, key)
	default:
		w.Header().Set("Allow", "GET, PUT, DELETE")
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Service) kvGet(w http.ResponseWriter, r *http.Request, qp QueryParams, key string) {
	value, raftIndex, err := s.store.GetKey(r.Context(), key,
		qp.Level(),
		qp.Freshness().Nanoseconds(),
		qp.FreshnessStrict(),
		qp.LinearizableTimeout(defaultLinearTimeout).Nanoseconds())
	if err != nil {
		if errors.Is(err, store.ErrKVKeyNotFound) {
			stats.Add(numKVNotFound, 1)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if s.handleKVNotLeader(w, r, qp, err) {
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", kvContentType)
	if qp.RaftIndex() && raftIndex > 0 {
		w.Header().Set("X-RQLITE-RAFT-INDEX", fmt.Sprintf("%d", raftIndex))
	}
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(value)))
	_, _ = w.Write(value)
}

func (s *Service) kvPut(w http.ResponseWriter, r *http.Request, qp QueryParams, key string) {
	if r.ContentLength > MaxKVValueBytes {
		http.Error(w, "value too large", http.StatusRequestEntityTooLarge)
		return
	}
	limited := io.LimitReader(r.Body, MaxKVValueBytes+1)
	value, err := io.ReadAll(limited)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}
	if len(value) > MaxKVValueBytes {
		http.Error(w, "value too large", http.StatusRequestEntityTooLarge)
		return
	}

	raftIndex, err := s.store.SetKey(r.Context(), key, value)
	if err != nil {
		if errors.Is(err, store.ErrKVEmptyKey) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if s.handleKVNotLeader(w, r, qp, err) {
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if qp.RaftIndex() {
		w.Header().Set("X-RQLITE-RAFT-INDEX", fmt.Sprintf("%d", raftIndex))
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Service) kvDelete(w http.ResponseWriter, r *http.Request, qp QueryParams, key string) {
	raftIndex, err := s.store.DeleteKey(r.Context(), key)
	if err != nil {
		if errors.Is(err, store.ErrKVKeyNotFound) {
			stats.Add(numKVNotFound, 1)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if errors.Is(err, store.ErrKVEmptyKey) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if s.handleKVNotLeader(w, r, qp, err) {
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if qp.RaftIndex() {
		w.Header().Set("X-RQLITE-RAFT-INDEX", fmt.Sprintf("%d", raftIndex))
	}
	w.WriteHeader(http.StatusOK)
}

// handleKVNotLeader handles the ErrNotLeader case for KV operations.
// Since the KV API is leader-only in V1, we always attempt to redirect to
// the leader rather than honoring the ?redirect query param. Returns true
// if a response was written (caller should return immediately).
func (s *Service) handleKVNotLeader(w http.ResponseWriter, r *http.Request, qp QueryParams, err error) bool {
	if !errors.Is(err, store.ErrNotLeader) {
		return false
	}
	rd, ferr := s.FormRedirect(r)
	if ferr != nil {
		http.Error(w, ferr.Error(), http.StatusServiceUnavailable)
		return true
	}
	http.Redirect(w, r, rd, http.StatusMovedPermanently)
	return true
}
