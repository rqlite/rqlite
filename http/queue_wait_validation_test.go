package http

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/queue"
)

func Test_QueuedExecuteWaitValidatesBody(t *testing.T) {
	for _, tc := range []struct {
		body   string
		status int
	}{
		{`["INSERT INTO foo VALUES(1)"] junk`, http.StatusBadRequest},
		{`["INSERT INTO foo VALUES(1)"] []`, http.StatusBadRequest},
		{`{}`, http.StatusBadRequest},
		{`[42]`, http.StatusBadRequest},
		{`[]`, http.StatusOK},
	} {
		t.Run(tc.body, func(t *testing.T) {
			s := New("", nil, nil, nil, nil)
			s.stmtQueue = queue.New[*command.Statement](1, 1, 0)
			defer s.stmtQueue.Close()
			done := make(chan struct{})
			defer close(done)
			go func() {
				select {
				case req := <-s.stmtQueue.C:
					req.Close()
				case <-done:
				}
			}()
			rr := httptest.NewRecorder()
			s.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/db/execute?queue&wait&noleader&timeout=1s", strings.NewReader(tc.body)))
			if rr.Code != tc.status {
				t.Fatalf("status=%d, want %d; body: %s", rr.Code, tc.status, rr.Body)
			}
		})
	}
}
