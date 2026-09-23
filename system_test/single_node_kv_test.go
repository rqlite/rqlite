package system

import (
	"bytes"
	"net/http"
	"strings"
	"testing"
)

func Test_SingleNodeKV_PutGetDelete(t *testing.T) {
	node := mustNewLeaderNode("kv-node")
	defer node.Deprovision()

	code, err := node.KVPut("alpha", []byte("hello world"))
	if err != nil {
		t.Fatalf("PUT failed: %s", err)
	}
	if code != http.StatusOK {
		t.Fatalf("PUT: got status %d, want 200", code)
	}

	code, body, err := node.KVGet("alpha")
	if err != nil {
		t.Fatalf("GET failed: %s", err)
	}
	if code != http.StatusOK {
		t.Fatalf("GET: got status %d, want 200", code)
	}
	if !bytes.Equal(body, []byte("hello world")) {
		t.Fatalf("GET returned %q, want %q", body, "hello world")
	}

	code, err = node.KVDelete("alpha")
	if err != nil {
		t.Fatalf("DELETE failed: %s", err)
	}
	if code != http.StatusOK {
		t.Fatalf("DELETE: got status %d, want 200", code)
	}

	code, _, err = node.KVGet("alpha")
	if err != nil {
		t.Fatalf("GET after DELETE failed: %s", err)
	}
	if code != http.StatusNotFound {
		t.Fatalf("GET after DELETE: got status %d, want 404", code)
	}
}

func Test_SingleNodeKV_GetMissing(t *testing.T) {
	node := mustNewLeaderNode("kv-node")
	defer node.Deprovision()

	code, _, err := node.KVGet("nothere")
	if err != nil {
		t.Fatalf("GET failed: %s", err)
	}
	if code != http.StatusNotFound {
		t.Fatalf("GET on fresh node: got %d, want 404", code)
	}

	// The lookup itself must not create the table.
	r, err := node.Query(`SELECT name FROM sqlite_master WHERE type='table' AND name='__rqlite_kv'`)
	if err != nil {
		t.Fatalf("sqlite_master query failed: %s", err)
	}
	if strings.Contains(r, "__rqlite_kv") {
		t.Fatalf("__rqlite_kv created by a GET on missing table: %s", r)
	}
}

func Test_SingleNodeKV_Overwrite(t *testing.T) {
	node := mustNewLeaderNode("kv-node")
	defer node.Deprovision()

	if code, err := node.KVPut("a", []byte("v1")); err != nil || code != http.StatusOK {
		t.Fatalf("first PUT failed (status %d): %v", code, err)
	}
	if code, err := node.KVPut("a", []byte("v2")); err != nil || code != http.StatusOK {
		t.Fatalf("second PUT failed (status %d): %v", code, err)
	}
	code, body, err := node.KVGet("a")
	if err != nil || code != http.StatusOK {
		t.Fatalf("GET failed (status %d): %v", code, err)
	}
	if !bytes.Equal(body, []byte("v2")) {
		t.Fatalf("overwrite GET returned %q, want %q", body, "v2")
	}
}

func Test_SingleNodeKV_Binary(t *testing.T) {
	node := mustNewLeaderNode("kv-node")
	defer node.Deprovision()

	val := make([]byte, 256)
	for i := range val {
		val[i] = byte(i)
	}
	if code, err := node.KVPut("bin", val); err != nil || code != http.StatusOK {
		t.Fatalf("PUT failed (status %d): %v", code, err)
	}
	code, body, err := node.KVGet("bin")
	if err != nil || code != http.StatusOK {
		t.Fatalf("GET failed (status %d): %v", code, err)
	}
	if !bytes.Equal(body, val) {
		t.Fatalf("binary round-trip mismatch")
	}
}

func Test_SingleNodeKV_DeleteIdempotent(t *testing.T) {
	node := mustNewLeaderNode("kv-node")
	defer node.Deprovision()

	if code, err := node.KVDelete("ghost"); err != nil || code != http.StatusNotFound {
		t.Fatalf("DELETE missing: status %d, err %v; want 404", code, err)
	}

	if code, err := node.KVPut("ghost", []byte("v")); err != nil || code != http.StatusOK {
		t.Fatalf("PUT failed (status %d): %v", code, err)
	}
	if code, err := node.KVDelete("ghost"); err != nil || code != http.StatusOK {
		t.Fatalf("DELETE after PUT: status %d, err %v; want 200", code, err)
	}
	if code, err := node.KVDelete("ghost"); err != nil || code != http.StatusNotFound {
		t.Fatalf("DELETE re-issued: status %d, err %v; want 404", code, err)
	}
}

func Test_SingleNodeKV_DropOnLastDelete(t *testing.T) {
	node := mustNewLeaderNode("kv-node")
	defer node.Deprovision()

	if code, err := node.KVPut("only", []byte("v")); err != nil || code != http.StatusOK {
		t.Fatalf("PUT failed (status %d): %v", code, err)
	}
	r, err := node.Query(`SELECT name FROM sqlite_master WHERE type='table' AND name='__rqlite_kv'`)
	if err != nil {
		t.Fatalf("sqlite_master query failed: %s", err)
	}
	if !strings.Contains(r, "__rqlite_kv") {
		t.Fatalf("__rqlite_kv missing after PUT: %s", r)
	}

	if code, err := node.KVDelete("only"); err != nil || code != http.StatusOK {
		t.Fatalf("DELETE failed (status %d): %v", code, err)
	}
	r, err = node.Query(`SELECT name FROM sqlite_master WHERE type='table' AND name='__rqlite_kv'`)
	if err != nil {
		t.Fatalf("sqlite_master query after drop failed: %s", err)
	}
	if strings.Contains(r, "__rqlite_kv") {
		t.Fatalf("__rqlite_kv still present after last DELETE: %s", r)
	}
}

func Test_SingleNodeKV_NoDropWhenOtherKeysRemain(t *testing.T) {
	node := mustNewLeaderNode("kv-node")
	defer node.Deprovision()

	if code, err := node.KVPut("a", []byte("av")); err != nil || code != http.StatusOK {
		t.Fatalf("PUT a failed (status %d): %v", code, err)
	}
	if code, err := node.KVPut("b", []byte("bv")); err != nil || code != http.StatusOK {
		t.Fatalf("PUT b failed (status %d): %v", code, err)
	}
	if code, err := node.KVDelete("a"); err != nil || code != http.StatusOK {
		t.Fatalf("DELETE a failed (status %d): %v", code, err)
	}

	r, err := node.Query(`SELECT name FROM sqlite_master WHERE type='table' AND name='__rqlite_kv'`)
	if err != nil {
		t.Fatalf("sqlite_master query failed: %s", err)
	}
	if !strings.Contains(r, "__rqlite_kv") {
		t.Fatalf("__rqlite_kv was dropped while another key remained: %s", r)
	}

	code, body, err := node.KVGet("b")
	if err != nil || code != http.StatusOK {
		t.Fatalf("GET b failed (status %d): %v", code, err)
	}
	if !bytes.Equal(body, []byte("bv")) {
		t.Fatalf("GET b returned %q, want %q", body, "bv")
	}
}

func Test_SingleNodeKV_KeyURLEscape(t *testing.T) {
	node := mustNewLeaderNode("kv-node")
	defer node.Deprovision()

	keys := []string{
		"with/slash",
		"with space",
		"with?question",
		"with#hash",
	}
	for _, k := range keys {
		if code, err := node.KVPut(k, []byte("v-"+k)); err != nil || code != http.StatusOK {
			t.Fatalf("PUT %q failed (status %d): %v", k, code, err)
		}
		code, body, err := node.KVGet(k)
		if err != nil || code != http.StatusOK {
			t.Fatalf("GET %q failed (status %d): %v", k, code, err)
		}
		if !bytes.Equal(body, []byte("v-"+k)) {
			t.Fatalf("GET %q returned %q, want %q", k, body, "v-"+k)
		}
	}
}

func Test_SingleNodeKV_LargeValue(t *testing.T) {
	node := mustNewLeaderNode("kv-node")
	defer node.Deprovision()

	val := bytes.Repeat([]byte{0xab}, 1<<20) // 1 MiB
	if code, err := node.KVPut("big", val); err != nil || code != http.StatusOK {
		t.Fatalf("PUT failed (status %d): %v", code, err)
	}
	code, body, err := node.KVGet("big")
	if err != nil || code != http.StatusOK {
		t.Fatalf("GET failed (status %d): %v", code, err)
	}
	if !bytes.Equal(body, val) {
		t.Fatalf("large round-trip mismatch (got %d bytes, want %d)", len(body), len(val))
	}
}

func Test_SingleNodeKV_DirectSQLRead(t *testing.T) {
	node := mustNewLeaderNode("kv-node")
	defer node.Deprovision()

	if code, err := node.KVPut("a", []byte("hello")); err != nil || code != http.StatusOK {
		t.Fatalf("PUT failed (status %d): %v", code, err)
	}
	// rqlite returns BLOB columns as base64 in JSON. "hello" → "aGVsbG8=".
	r, err := node.Query(`SELECT v FROM __rqlite_kv WHERE k = 'a'`)
	if err != nil {
		t.Fatalf("SQL query failed: %s", err)
	}
	if !strings.Contains(r, "aGVsbG8=") {
		t.Fatalf("direct SQL read did not include base64(hello): %s", r)
	}
	if !strings.Contains(r, `"types":["blob"]`) {
		t.Fatalf("direct SQL read did not report column type blob: %s", r)
	}
}

func Test_SingleNodeKV_EmptyKey(t *testing.T) {
	node := mustNewLeaderNode("kv-node")
	defer node.Deprovision()

	// PUT /kv/ (no key) -> 400.
	resp, err := http.DefaultClient.Do(mustNewKVRequest(t, http.MethodPut, node.APIAddr, "", []byte("v")))
	if err != nil {
		t.Fatalf("PUT /kv/ failed: %s", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("PUT /kv/: got %d, want 400", resp.StatusCode)
	}
}

func mustNewKVRequest(t *testing.T, method, apiAddr, key string, body []byte) *http.Request {
	t.Helper()
	u := "http://" + apiAddr + "/kv/" + key
	var br *bytes.Reader
	if body != nil {
		br = bytes.NewReader(body)
	}
	var req *http.Request
	var err error
	if br != nil {
		req, err = http.NewRequest(method, u, br)
	} else {
		req, err = http.NewRequest(method, u, nil)
	}
	if err != nil {
		t.Fatalf("failed to build request: %s", err)
	}
	return req
}
