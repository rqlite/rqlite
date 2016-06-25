package env

import (
	"os"
	"testing"
)

func Test_OverrideUInt(t *testing.T) {
	i := uint64(1)
	os.Setenv("INT", "2")
	if err := Override(&i, "INT"); err != nil {
		t.Fatalf("failed to override INT: %s", err.Error())
	}
	if i != 2 {
		t.Fatalf("INT not overridden, exp %d, got %d", 2, i)
	}

	if err := Override(&i, "INT_XXX"); err != nil {
		t.Fatalf("failed to override INT: %s", err.Error())
	}
	if i != 2 {
		t.Fatalf("INT overridden, exp %d, got %d", 2, i)
	}
}

func Test_OverrideString(t *testing.T) {
	s := "something"
	os.Setenv("STRING", "else")
	if err := Override(&s, "STRING"); err != nil {
		t.Fatalf("failed to override STRING: %s", err.Error())
	}
	if s != "else" {
		t.Fatalf("STRING not overridden, exp %s, got %s", "else", s)
	}

	if err := Override(&s, "STRING_XXX"); err != nil {
		t.Fatalf("failed to override STRING: %s", err.Error())
	}
	if s != "else" {
		t.Fatalf("STRING overridden, exp %s, got %s", "else", s)
	}
}

func Test_OverrideBool(t *testing.T) {
	b := true
	os.Setenv("BOOL", "false")
	if err := Override(&b, "BOOL"); err != nil {
		t.Fatalf("failed to override BOOL: %s", err.Error())
	}
	if b != false {
		t.Fatalf("BOOL not overridden, exp %v, got %v", false, b)
	}

	if err := Override(&b, "BOOL_XXX"); err != nil {
		t.Fatalf("failed to override BOOL: %v", err.Error())
	}
	if b != false {
		t.Fatalf("BOOL overridden, exp %v, got %v", false, b)
	}
}

func Test_OverrideUnsupported(t *testing.T) {
	i := 0
	os.Setenv("FOO", "BAR")
	if err := Override(i, "FOO"); err == nil {
		t.Fatal("expected error for unsupported type")
	}
}
