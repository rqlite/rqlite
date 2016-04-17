package http

import (
	"strings"
	"testing"
)

type testBasicAuther struct {
	ok       bool
	username string
	password string
}

func (t *testBasicAuther) BasicAuth() (string, string, bool) {
	return t.username, t.password, t.ok
}

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

func Test_AuthLoadSingleRequest(t *testing.T) {
	const jsonStream = `
		[
			{"username": "username1", "password": "password1"}
		]
	`

	store := NewCredentialsStore()
	if err := store.Load(strings.NewReader(jsonStream)); err != nil {
		t.Fatalf("failed to load multiple credentials: %s", err.Error())
	}

	b1 := &testBasicAuther{
		username: "username1",
		password: "password1",
		ok:       true,
	}
	b2 := &testBasicAuther{
		username: "username1",
		password: "wrong",
		ok:       true,
	}
	b3 := &testBasicAuther{}

	if check := store.CheckRequest(b1); !check {
		t.Fatalf("username1 (b1) credential not checked correctly via request")
	}
	if check := store.CheckRequest(b2); check {
		t.Fatalf("username1 (b2) credential not checked correctly via request")
	}
	if check := store.CheckRequest(b3); check {
		t.Fatalf("username1 (b3) credential not checked correctly via request")
	}
}
