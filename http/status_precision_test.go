package http

import "testing"

func Test_getSubJSON_PreservesNumbers(t *testing.T) {
	input := []byte(`{"store":{"index":9007199254740993,"max":18446744073709551615}}`)
	for _, tc := range []struct{ key, want string }{
		{"store.index", "9007199254740993"},
		{"store.max", "18446744073709551615"},
		{"store", `{"index":9007199254740993,"max":18446744073709551615}`},
	} {
		t.Run(tc.key, func(t *testing.T) {
			got, err := getSubJSON(input, tc.key)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.want {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
}

func Test_getSubJSON_RejectsTrailingData(t *testing.T) {
	for _, input := range []string{`{"store":1} {}`, `{"store":1} junk`} {
		if _, err := getSubJSON([]byte(input), "store"); err == nil {
			t.Fatalf("expected error for %q", input)
		}
	}
}
