package cdctest

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"

	cdcjson "github.com/rqlite/rqlite/v8/cdc/json"
)

// HTTPTestServer is a test server that simulates an HTTP endpoint for receiving CDC messages.
// It captures incoming requests and stores the messages for later verification.
type HTTPTestServer struct {
	*httptest.Server
	requests [][]byte
	messages map[uint64]*cdcjson.CDCMessage
	mu       sync.Mutex
}

// NewHTTPTestServer creates a new instance of HTTPTestServer.
func NewHTTPTestServer() *HTTPTestServer {
	hts := &HTTPTestServer{
		messages: make(map[uint64]*cdcjson.CDCMessage),
	}
	hts.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hts.mu.Lock()
		defer hts.mu.Unlock()

		defer r.Body.Close()
		body, err := io.ReadAll(r.Body)
		if err != nil || len(body) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		hts.requests = append(hts.requests, body)
		var envelope cdcjson.CDCMessagesEnvelope
		if err := cdcjson.UnmarshalFromEnvelopeJSON(body, &envelope); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		for _, msg := range envelope.Payload {
			hts.messages[msg.Index] = msg
		}

		w.WriteHeader(http.StatusOK)
	}))
	return hts
}

// GetRequests returns a copy of the requests received by the server.
func (h *HTTPTestServer) GetRequests() [][]byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := make([][]byte, len(h.requests))
	copy(result, h.requests)
	return result
}

// GetRequestCount returns the number of requests received by the server.
func (h *HTTPTestServer) GetRequestCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.requests)
}

// ClearRequests clears the stored requests.
func (h *HTTPTestServer) ClearRequests() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.requests = nil
}

// GetMessageCount returns the number of unique messages received by the server.
func (h *HTTPTestServer) GetMessageCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.messages)
}

// CheckMessagesExist checks if messages with indices from 1 to n exist in the server's stored messages.
// It returns true if all messages exist, false otherwise.
func (h *HTTPTestServer) CheckMessagesExist(n int) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := 1; i <= n; i++ {
		if _, ok := h.messages[uint64(i)]; !ok {
			return false
		}
	}
	return true
}
