package history

import (
	"bytes"
	"io"
	"reflect"
	"strings"
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
			orig: []string{"foo", "foo", "foo", "bar", "bar", "bar"},
			exp:  []string{"foo", "bar"},
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
}

func Test_Filter(t *testing.T) {
	for i, tt := range []struct {
		orig []string
		exp  []string
	}{
		{
			orig: nil,
			exp:  nil,
		},
		{
			orig: []string{""},
			exp:  []string{},
		},
		{
			orig: []string{"    "},
			exp:  []string{},
		},
		{
			orig: []string{"    ", ""},
			exp:  []string{},
		},
		{
			orig: []string{"foo"},
			exp:  []string{"foo"},
		},
		{
			orig: []string{"foo", ""},
			exp:  []string{"foo"},
		},
		{
			orig: []string{"foo", "", "      "},
			exp:  []string{"foo"},
		},
		{
			orig: []string{"foo", "", "      ", "bar"},
			exp:  []string{"foo", "bar"},
		},
		{
			orig: []string{"", "foo", "", "      ", "bar"},
			exp:  []string{"foo", "bar"},
		},
	} {
		got := Filter(tt.orig)
		if !reflect.DeepEqual(tt.exp, got) {
			t.Fatalf("test %d failed, exp %s, got %s", i, tt.exp, got)
		}

	}
}

func Test_Read(t *testing.T) {
	for i, tt := range []struct {
		r   io.Reader
		exp []string
	}{
		{
			r:   nil,
			exp: nil,
		},
		{
			r:   strings.NewReader(""),
			exp: []string{},
		},
		{
			r:   strings.NewReader("\n"),
			exp: []string{},
		},
		{
			r:   strings.NewReader("\n\n\n"),
			exp: []string{},
		},
		{
			r:   strings.NewReader("\n\n     \n"),
			exp: []string{},
		},
		{
			r:   strings.NewReader("SELECT"),
			exp: []string{"SELECT"},
		},
		{
			r:   strings.NewReader("SELECT\nINSERT\n.schema"),
			exp: []string{"SELECT", "INSERT", ".schema"},
		},
		{
			r:   strings.NewReader("SELECT\nINSERT\n.schema\n"),
			exp: []string{"SELECT", "INSERT", ".schema"},
		},
		{
			r:   strings.NewReader("SELECT\nINSERT\n\n.schema\n"),
			exp: []string{"SELECT", "INSERT", ".schema"},
		},
		{
			r:   strings.NewReader("SELECT\nINSERT\n\n     \n.schema\n"),
			exp: []string{"SELECT", "INSERT", ".schema"},
		},
	} {
		got, err := Read(tt.r)
		if err != nil {
			t.Fatalf("test %d failed, got error: %s", i, err.Error())
		}
		if !reflect.DeepEqual(tt.exp, got) {
			t.Fatalf("test %d failed, exp %s, got %s", i, tt.exp, got)
		}
	}
}

func Test_Write(t *testing.T) {
	for i, tt := range []struct {
		h   []string
		sz  int
		w   *bytes.Buffer
		exp string
	}{
		{
			h:   []string{"SELECT * FROM foo"},
			sz:  100,
			w:   new(bytes.Buffer),
			exp: "SELECT * FROM foo",
		},
		{
			h:   []string{"SELECT", "INSERT"},
			sz:  100,
			w:   new(bytes.Buffer),
			exp: "SELECT\nINSERT",
		},
		{
			h:   []string{"SELECT", "INSERT", "INSERT", "UPDATE"},
			sz:  100,
			w:   new(bytes.Buffer),
			exp: "SELECT\nINSERT\nUPDATE",
		},
		{
			h:   []string{"SELECT *", "INSERT INTO", "INSERT INTO", "UPDATE", "", "DELETE"},
			sz:  100,
			w:   new(bytes.Buffer),
			exp: "SELECT *\nINSERT INTO\nUPDATE\nDELETE",
		},
		{
			h:   []string{"SELECT *", "INSERT INTO", "INSERT INTO", "UPDATE", "", "DELETE"},
			sz:  2,
			w:   new(bytes.Buffer),
			exp: "UPDATE\nDELETE",
		},
		{
			h:   []string{"SELECT *", "INSERT INTO", "INSERT INTO", "UPDATE", "", "DELETE"},
			sz:  0,
			w:   new(bytes.Buffer),
			exp: "",
		},
	} {
		if err := Write(tt.h, tt.sz, tt.w); err != nil {
			t.Fatalf("test %d failed, got error: %s", i, err.Error())
		}
		got := tt.w.String()
		if !reflect.DeepEqual(tt.exp, got) {
			t.Fatalf("test %d failed, exp %s, got %s", i, tt.exp, got)
		}
	}
}
