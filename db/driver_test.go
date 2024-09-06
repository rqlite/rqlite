package db

import (
	"testing"
)

func Test_DefaultDriver(t *testing.T) {
	d := DefaultDriver()
	if d == nil {
		t.Fatalf("DefaultDriver returned nil")
	}
	if d.Name() != defaultDriverName {
		t.Fatalf("DefaultDriver returned incorrect name: %s", d.Name())
	}

	// Call it again, make sure it doesn't panic.
	d = DefaultDriver()
	if d == nil {
		t.Fatalf("DefaultDriver returned nil")
	}
}

func Test_NewDriver(t *testing.T) {
	name := "test-driver"
	extensions := []string{"test1", "test2"}
	d := NewDriver(name, extensions, CnkOnCloseModeEnabled)
	if d == nil {
		t.Fatalf("NewDriver returned nil")
	}
	if d.Name() != name {
		t.Fatalf("NewDriver returned incorrect name: %s", d.Name())
	}
	if len(d.Extensions()) != 2 {
		t.Fatalf("NewDriver returned incorrect extensions: %v", d.Extensions())
	}
}
