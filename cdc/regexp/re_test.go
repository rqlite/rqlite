package regexp

import (
	"encoding/json"
	"testing"
)

func Test_JSONRoundTrip(t *testing.T) {
	type cfg struct {
		R Regexp `json:"r,omitempty"`
	}

	in := cfg{R: MustCompile(`^a+$`)}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var out cfg
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if out.R.Regexp == nil || out.R.String() != `^a+$` {
		t.Fatalf("round-trip mismatch: got %q", out.R.String())
	}
	if !out.R.MatchString("aaa") || out.R.MatchString("aba") {
		t.Fatalf("compiled regex behaves incorrectly")
	}
}

func Test_NullAndEmpty(t *testing.T) {
	type cfg struct {
		R Regexp `json:"r,omitempty"`
	}

	// null -> unset
	var c1 cfg
	if err := json.Unmarshal([]byte(`{"r":null}`), &c1); err != nil {
		t.Fatal(err)
	}
	if !c1.R.IsZero() {
		t.Fatalf("expected zero after null")
	}

	// empty string -> unset
	var c2 cfg
	if err := json.Unmarshal([]byte(`{"r":""}`), &c2); err != nil {
		t.Fatal(err)
	}
	if !c2.R.IsZero() {
		t.Fatalf("expected zero after empty string")
	}
}
