package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/proxy"
)

func Test_LoadTimings(t *testing.T) {
	for _, body := range []string{"SELECT 1", "SQLite format 3\x00"} {
		t.Run(body, func(t *testing.T) {
			m := &MockStore{}
			c := &mockClusterService{}
			s := New("", m, c, proxy.New(m, c), nil)
			rr := httptest.NewRecorder()
			start := time.Now()
			s.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/db/load?timings", strings.NewReader(body)))
			elapsed := time.Since(start).Seconds()
			if rr.Code != http.StatusOK {
				t.Fatalf("status %d: %s", rr.Code, rr.Body)
			}
			var response struct {
				Time *float64 `json:"time"`
			}
			if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if response.Time == nil || *response.Time < 0 || *response.Time > elapsed {
				t.Fatalf("expected load time between 0 and %g seconds, got %s", elapsed, rr.Body)
			}
		})
	}
}
