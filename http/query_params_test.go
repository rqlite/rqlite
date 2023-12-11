package http

import (
	"net/http"
	"net/url"
	"reflect"
	"testing"
)

// Test_NewQueryParams tests the NewQueryParams function for various scenarios.
func Test_NewQueryParams(t *testing.T) {
	testCases := []struct {
		name        string
		rawQuery    string
		expected    QueryParams
		expectError bool
	}{
		{"Empty Query", "", QueryParams{}, false},
		{"Valid Query", "timeout=10s&chunk_kb=1024&q=test", QueryParams{"timeout": "10s", "chunk_kb": "1024", "q": "test"}, false},
		{"Invalid Timeout", "timeout=invalid", nil, true},
		{"Invalid ChunkKB", "chunk_kb=invalid", nil, true},
		{"Empty Q", "q=", nil, true},
		{"Invalid Q", "q", nil, true},
		{"Valid Q, no case changes", "q=SELeCT", QueryParams{"q": "SELeCT"}, false},
		{"Multiple Values", "key1=value1&key2=value2", QueryParams{"key1": "value1", "key2": "value2"}, false},
		{"Mixed Case Keys", "KeyOne=value1&keyTwo=value2", QueryParams{"KeyOne": "value1", "keyTwo": "value2"}, false},
		{"Numeric Values", "num=1234", QueryParams{"num": "1234"}, false},
		{"Special Characters", "special=%40%23%24", QueryParams{"special": "@#$"}, false},
		{"Multiple Same Keys", "key=same&key=different", QueryParams{"key": "same"}, false},
		{"No Value", "key=", QueryParams{"key": ""}, false},
		{"Complex Query", "a=1&b=two&c=&d=true&e=123.456", QueryParams{"a": "1", "b": "two", "c": "", "d": "true", "e": "123.456"}, false},
		{"Invalid URL Encoding", "invalid=%ZZ", nil, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := &http.Request{
				URL: &url.URL{RawQuery: tc.rawQuery},
			}

			qp, err := NewQueryParams(req)
			if (err != nil) != tc.expectError {
				t.Errorf("Test '%s' failed: expected error: %v, got: %v", tc.name, tc.expectError, err)
			}
			if err == nil && !reflect.DeepEqual(qp, tc.expected) {
				t.Errorf("Test '%s' failed: expected: %#v, got: %#v", tc.name, tc.expected, qp)
			}
		})
	}
}
