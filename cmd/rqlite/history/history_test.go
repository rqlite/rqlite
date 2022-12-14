package history

import (
	"reflect"
	"testing"
)

func Test_Dedupe(t *testing.T) {
	for i, tt := range []struct {
		orig []string
		exp  []string
	}{
		{
			orig: nil,
			exp:  nil,
		},
		{
			orig: []string{"foo"},
			exp:  []string{"foo"},
		},
		{
			orig: []string{"foo", "bar"},
			exp:  []string{"foo", "bar"},
		},
		{
			orig: []string{"foo", "bar", "foo"},
			exp:  []string{"foo", "bar", "foo"},
		},
		{
			orig: []string{"foo", "foo", "foo"},
			exp:  []string{"foo"},
		},
		{
			orig: []string{"foo", "bar", "foo", "foo"},
			exp:  []string{"foo", "bar", "foo"},
		},
		{
			orig: []string{"foo", "foo", "bar", "foo", "foo"},
			exp:  []string{"foo", "bar", "foo"},
		},
		{
			orig: []string{"foo", "foo", "bar", "foo", "foo", "qux"},
			exp:  []string{"foo", "bar", "foo", "qux"},
		},
		{
			orig: []string{"foo", "foo", "bar", "bar", "foo", "foo", "qux"},
			exp:  []string{"foo", "bar", "foo", "qux"},
		},
	} {
		got := Dedupe(tt.orig)
		if !reflect.DeepEqual(tt.exp, got) {
			t.Fatalf("test %d failed, exp %s, got %s", i, tt.exp, got)
		}

	}
	return
}
