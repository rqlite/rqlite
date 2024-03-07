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
		{"Valid Query", "timeout=10s&q=test", QueryParams{"timeout": "10s", "q": "test"}, false},
		{"Invalid Timeout", "timeout=invalid", nil, true},
		{"Invalid Retry", "retries=invalid", nil, true},
		{"Valid Retry", "retries=4", QueryParams{"retries": "4"}, false},
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
		{"freshness_strict", "&freshness=5s&freshness_strict", QueryParams{"freshness_strict": "", "freshness": "5s"}, false},
		{"freshness_strict requires freshness", "freshness_strict", nil, true},
		{"Sync with timeout", "sync&timeout=2s", QueryParams{"sync": "", "timeout": "2s"}, false},
		{"Byte array with associative", "byte_array&associative", QueryParams{"byte_array": "", "associative": ""}, false},
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

// Test_QueryParamsTimings tests that looking up unset timings values does
// not result in a panic, and that zero values are returned.
func Test_NewQueryParamsTimes(t *testing.T) {
	qp := QueryParams{}
	if qp.Freshness() != 0 {
		t.Errorf("Expected 0, got %v", qp.Freshness())
	}
}
