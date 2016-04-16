package http

import (
	"strings"
	"testing"
)

func Test_AuthLoadSingle(t *testing.T) {
	const jsonStream = `
		[
			{"username": "username1", "password": "password1"}
		]
	`

	store := NewCredentialsStore()
	if err := store.Load(strings.NewReader(jsonStream)); err != nil {
		t.Fatalf("failed to load single credential: %s", err.Error())
	}

	if check := store.Check("username1", "password1"); !check {
		t.Fatalf("single credential not loaded correctly")
	}
	if check := store.Check("username1", "wrong"); check {
		t.Fatalf("single credential not loaded correctly")
	}

	if check := store.Check("wrong", "password1"); check {
		t.Fatalf("single credential not loaded correctly")
	}
	if check := store.Check("wrong", "wrong"); check {
		t.Fatalf("single credential not loaded correctly")
	}
}

func Test_AuthLoadMultiple(t *testing.T) {
	const jsonStream = `
		[
			{"username": "username1", "password": "password1"},
			{"username": "username2", "password": "password2"}
		]
	`

	store := NewCredentialsStore()
	if err := store.Load(strings.NewReader(jsonStream)); err != nil {
		t.Fatalf("failed to load multiple credentials: %s", err.Error())
	}

	if check := store.Check("username1", "password1"); !check {
		t.Fatalf("username1 credential not loaded correctly")
	}
	if check := store.Check("username1", "password2"); check {
		t.Fatalf("username1 credential not loaded correctly")
	}

	if check := store.Check("username2", "password2"); !check {
		t.Fatalf("username2 credential not loaded correctly")
	}
	if check := store.Check("username2", "password1"); check {
		t.Fatalf("username2 credential not loaded correctly")
	}

	if check := store.Check("username1", "wrong"); check {
		t.Fatalf("multiple credential not loaded correctly")
	}
	if check := store.Check("wrong", "password1"); check {
		t.Fatalf("multiple credential not loaded correctly")
	}
	if check := store.Check("wrong", "wrong"); check {
		t.Fatalf("multiple credential not loaded correctly")
	}
}