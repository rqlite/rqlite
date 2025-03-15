package proto

import (
	"testing"
)

// Test_Credentials tests the Credentials methods on a nil Credentials,
// ensuring it doesn't panic and instead returns empty strings.
func Test_NilCredentials(t *testing.T) {
	var creds *Credentials
	if creds.GetUsername() != "" {
		t.Fatalf("expected empty username")
	}
	if creds.GetPassword() != "" {
		t.Fatalf("expected empty password")
	}
}
