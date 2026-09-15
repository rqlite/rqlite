package cdctest

import (
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"

	cdcjson "github.com/rqlite/rqlite/v10/cdc/json"
)

// HTTPTestServer is a test server that simulates an HTTP endpoint for receiving CDC messages.
// It captures incoming requests and stores the messages for later verification.
type HTTPTestServer struct {
	*httptest.Server

	requests [][]byte
	messages map[uint64]*cdcjson.CDCMessage
	mu       sync.Mutex

	failRate int // Percentage of requests to fail (0-100)
	numFail  atomic.Int64

	DumpRequest bool
}

// NewHTTPTestServer creates a new instance of HTTPTestServer.
func NewHTTPTestServer() *HTTPTestServer {
	hts := &HTTPTestServer{
		messages: make(map[uint64]*cdcjson.CDCMessage),
	}
	hts.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hts.mu.Lock()
		defer hts.mu.Unlock()

		if hts.failRate > 0 && (rand.Intn(100) < hts.failRate) {
			hts.numFail.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

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

		if hts.DumpRequest {
			fmt.Println(string(body))
		}

		w.WriteHeader(http.StatusOK)
	}))

	hts.Listener = mustLocalListen()
	return hts
}

// URL returns the URL of the test server.
func (h *HTTPTestServer) URL() string {
	return fmt.Sprintf("http://%s", h.Server.Listener.Addr().String())
}

// Start starts the test server.
func (h *HTTPTestServer) Start() {
	h.Server.Start()
}

// Close stops and closes the test server.
func (h *HTTPTestServer) Close() {
	h.Server.Close()
}

// SetFailRate sets the percentage of requests that should fail (0-100).
func (h *HTTPTestServer) SetFailRate(rate int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if rate < 0 {
		rate = 0
	} else if rate > 100 {
		rate = 100
	}
	h.failRate = rate
}

// GetRequests returns a copy of the requests received by the server.
func (h *HTTPTestServer) GetRequests() [][]byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.requests)
}

// GetRequestCount returns the number of requests received by the server.
func (h *HTTPTestServer) GetRequestCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.requests)
}

// GetFailedRequestCount returns the number of requests that failed due to simulated failures.
func (h *HTTPTestServer) GetFailedRequestCount() int64 {
	return h.numFail.Load()
}

// GetHighestMessageIndex returns the highest message index received by the server.
func (h *HTTPTestServer) GetHighestMessageIndex() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	var highest uint64
	for index := range h.messages {
		if index > highest {
			highest = index
		}
	}
	return highest
}

// Reset delete all received data.
func (h *HTTPTestServer) Reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.requests = nil
	h.messages = make(map[uint64]*cdcjson.CDCMessage)
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

func mustLocalListen() net.Listener {
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		panic(err)
	}
	return ln
}
