package auth

import (
	"os"
	"strings"
	"testing"

	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

type testBasicAuther struct {
	ok       bool
	username string
	password string
}

func (t *testBasicAuther) BasicAuth() (string, string, bool) {
	return t.username, t.password, t.ok
}

func Test_AuthLoadEmpty(t *testing.T) {
	const jsonStream = `[]`

	store := NewCredentialsStore()
	if err := store.Load(strings.NewReader(jsonStream)); err != nil {
		t.Fatalf("failed to load empty JSON: %s", err.Error())
	}
}

func Test_AuthLoadMalformed(t *testing.T) {
	const jsonStream = `
		[
			{
				"username": "username1",
				"password": "password1",
				"perms": ["foo", "bar"
			}
		]
	`

	store := NewCredentialsStore()
	if err := store.Load(strings.NewReader(jsonStream)); err == nil {
		t.Fatalf("expected error for malformed JSON input")
	}
}

func Test_AuthLoadMissingUsername(t *testing.T) {
	const jsonStream = `
		[
			{
				"password": "password1",
				"perms": ["foo", "bar"]
			}
		]
	`

	store := NewCredentialsStore()
	if err := store.Load(strings.NewReader(jsonStream)); err == nil {
		t.Fatalf("expected error for JSON input with missing username")
	}
}

func Test_AuthLoadMissingPassword(t *testing.T) {
	const jsonStream = `
		[
			{
				"username": "username1",
				"perms": ["foo", "bar"]
			}
		]
	`

	store := NewCredentialsStore()
	if err := store.Load(strings.NewReader(jsonStream)); err == nil {
		t.Fatalf("expected error for JSON input with missing password")
	}
}

func Test_AuthLoadMissingPerms(t *testing.T) {
	const jsonStream = `
		[
			{
				"username": "username1",
				"password": "password1"
			}
		]
	`

	store := NewCredentialsStore()
	if err := store.Load(strings.NewReader(jsonStream)); err == nil {
		t.Fatalf("expected error for JSON input with missing perms")
	}
}

func Test_AuthLoadEmptyPerms(t *testing.T) {
	const jsonStream = `
		[
			{
				"username": "username1",
				"password": "password1",
				"perms": []
			}
		]
	`

	store := NewCredentialsStore()
	if err := store.Load(strings.NewReader(jsonStream)); err == nil {
		t.Fatalf("expected error for JSON input with empty perms")
	}
}

// Test_AuthLoadMissingFieldsNotInherited ensures that a field missing from a
// later entry is rejected, rather than silently taking the value from the
// previous entry.
func Test_AuthLoadMissingFieldsNotInherited(t *testing.T) {
	for _, tc := range []struct {
		name       string
		jsonStream string
	}{
		{
			name: "password",
			jsonStream: `
		[
			{"username": "username1", "password": "password1", "perms": ["foo"]},
			{"username": "username2", "perms": ["bar"]}
		]
	`,
		},
		{
			name: "perms",
			jsonStream: `
		[
			{"username": "username1", "password": "password1", "perms": ["foo"]},
			{"username": "username2", "password": "password2"}
		]
	`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := NewCredentialsStore()
			if err := store.Load(strings.NewReader(tc.jsonStream)); err == nil {
				t.Fatalf("expected error for second entry with missing %s", tc.name)
			}
		})
	}
}

// Test_AuthLoadNotArray ensures that input which is not exactly one JSON array
// is rejected, rather than being silently accepted as an empty or truncated
// set of credentials.
func Test_AuthLoadNotArray(t *testing.T) {
	for _, tc := range []struct {
		name       string
		jsonStream string
	}{
		{
			name:       "empty object",
			jsonStream: `{}`,
		},
		{
			name: "trailing array",
			jsonStream: `
			[{"username": "username1", "password": "password1", "perms": ["foo"]}]
			[{"username": "username2", "password": "password2", "perms": ["all"]}]
		`,
		},
		{
			name: "trailing garbage",
			jsonStream: `
			[{"username": "username1", "password": "password1", "perms": ["foo"]}]
			garbage
		`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := NewCredentialsStore()
			if err := store.Load(strings.NewReader(tc.jsonStream)); err == nil {
				t.Fatalf("expected error for input that is not a single JSON array")
			}
		})
	}
}

// Test_AuthLoadDuplicateUsername ensures that a username appearing more than
// once is rejected, rather than the later entry silently replacing the earlier
// one's password and permissions.
func Test_AuthLoadDuplicateUsername(t *testing.T) {
	const jsonStream = `
		[
			{"username": "username1", "password": "password1", "perms": ["query"]},
			{"username": "username2", "password": "password2", "perms": ["query"]},
			{"username": "username1", "password": "password3", "perms": ["all"]}
		]
	`

	store := NewCredentialsStore()
	if err := store.Load(strings.NewReader(jsonStream)); err == nil {
		t.Fatalf("expected error for duplicate username")
	}
}

func Test_AuthLoadErrorPreservesCredentials(t *testing.T) {
	for _, suffix := range []string{
		`, {"username":"invalid"}]`,
		`, {"username":"*","perms":["query"]}]`,
		`, {"username":"existing","password":"changed","perms":["all"]}]`,
		`] trailing`,
		`] []`,
		``,
	} {
		t.Run(suffix, func(t *testing.T) {
			store := NewCredentialsStore()
			if err := store.Load(strings.NewReader(`[{"username":"existing","password":"secret","perms":["query"]}]`)); err != nil {
				t.Fatal(err)
			}
			input := `[{"username":"existing","password":"changed","perms":["all"]},{"username":"new","password":"new-secret","perms":["all"]},{"username":"*","perms":["all"]}` + suffix
			if err := store.Load(strings.NewReader(input)); err == nil {
				t.Fatal("expected invalid credentials to be rejected")
			}
			if store.AA("", "", PermExecute) {
				t.Error("rejected load enabled anonymous execution")
			}
			if store.Check("new", "new-secret") || store.HasPerm("new", PermAll) {
				t.Error("rejected load installed new credentials or permissions")
			}
			if !store.AA("existing", "secret", PermQuery) || store.AA("existing", "secret", PermExecute) {
				t.Error("rejected load changed existing authorization")
			}
		})
	}
}

func Test_AuthLoadReplacesCredentials(t *testing.T) {
	for _, replacement := range []struct {
		name, input, username, password string
	}{
		{"new user", `[{"username":"new","password":"new-secret","perms":["status"]}]`, "new", "new-secret"},
		{"existing user", `[{"username":"existing","password":"changed","perms":["status"]}]`, "existing", "changed"},
		{"empty", `[]`, "", ""},
	} {
		t.Run(replacement.name, func(t *testing.T) {
			store := NewCredentialsStore()
			initial := `[{"username":"existing","password":"secret","perms":["query"]},{"username":"obsolete","password":"old-secret","perms":["all"]},{"username":"*","perms":["execute"]}]`
			if err := store.Load(strings.NewReader(initial)); err != nil {
				t.Fatal(err)
			}
			if err := store.Load(strings.NewReader(replacement.input)); err != nil {
				t.Fatal(err)
			}
			for _, user := range []string{"existing", "obsolete", "new", AllUsers} {
				password, ok := store.Password(user)
				if user == replacement.username {
					if !ok || password != replacement.password || !store.AA(user, replacement.password, PermStatus) {
						t.Errorf("replacement credentials not active for %q", user)
					}
				} else if ok || store.HasPerm(user, PermStatus) {
					t.Errorf("replacement retained credentials or permissions for %q", user)
				}
				if store.HasAnyPerm(user, PermQuery, PermExecute, PermAll) {
					t.Errorf("replacement retained old permissions for %q", user)
				}
			}
			if store.Check("existing", "secret") || store.AA("", "", PermExecute) {
				t.Error("replacement retained old password or anonymous access")
			}
		})
	}
}

func Test_AuthLoadSingle(t *testing.T) {
	const jsonStream = `
		[
			{"username": "username1", "password": "password1", "perms": ["all"]}
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

	var pw string
	var ok bool
	pw, ok = store.Password("username1")
	if pw != "password1" || !ok {
		t.Fatalf("wrong password returned")
	}
	_, ok = store.Password("nonsense")
	if ok {
		t.Fatalf("password returned for nonexistent user")
	}
}

func Test_AuthLoadMultiple(t *testing.T) {
	const jsonStream = `
		[
			{"username": "username1", "password": "password1", "perms": ["all"]},
			{"username": "username2", "password": "password2", "perms": ["all"]}
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
			{"username": "username1", "password": "password1", "perms": ["all"]}
		]
	`

	store := NewCredentialsStore()
	if err := store.Load(strings.NewReader(jsonStream)); err != nil {
		t.Fatalf("failed to load single credential: %s", err.Error())
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

func Test_AuthPermsLoadSingle(t *testing.T) {
	const jsonStream = `
		[
			{
				"username": "username1",
				"password": "password1",
				"perms": ["foo", "bar"]
			},
			{
				"username": "username2",
				"password": "password1",
				"perms": ["baz"]
			}
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

	if perm := store.HasPerm("wrong", "foo"); perm {
		t.Fatalf("wrong has foo perm")
	}

	if perm := store.HasPerm("username1", "foo"); !perm {
		t.Fatalf("username1 does not have foo perm")
	}
	if perm := store.HasPerm("username1", "bar"); !perm {
		t.Fatalf("username1 does not have bar perm")
	}
	if perm := store.HasPerm("username1", "baz"); perm {
		t.Fatalf("username1 does have baz perm")
	}

	if perm := store.HasPerm("username2", "baz"); !perm {
		t.Fatalf("username2 does not have baz perm")
	}

	if perm := store.HasAnyPerm("username1", "foo"); !perm {
		t.Fatalf("username1 does not have foo perm")
	}
	if perm := store.HasAnyPerm("username1", "bar"); !perm {
		t.Fatalf("username1 does not have bar perm")
	}
	if perm := store.HasAnyPerm("username1", "foo", "bar"); !perm {
		t.Fatalf("username1 does not have foo or bar perm")
	}
	if perm := store.HasAnyPerm("username1", "foo", "qux"); !perm {
		t.Fatalf("username1 does not have foo or qux perm")
	}
	if perm := store.HasAnyPerm("username1", "qux", "bar"); !perm {
		t.Fatalf("username1 does not have bar perm")
	}
	if perm := store.HasAnyPerm("username1", "baz", "qux"); perm {
		t.Fatalf("username1 has baz or qux perm")
	}
}

func Test_AuthPermsAANilStore(t *testing.T) {
	var store *CredentialsStore
	if !store.AA("username1", "password1", "foo") {
		t.Fatalf("nil store didn't authorize")
	}
}

func Test_AuthPermsAA(t *testing.T) {
	const jsonStream = `
		[
			{
				"username": "username1",
				"password": "password1",
				"perms": ["foo", "bar"]
			},
			{
				"username": "username2",
				"password": "password2",
				"perms": ["baz"]
			},
			{
				"username": "*",
				"perms": ["qux"]
			}
		]
	`

	store := NewCredentialsStore()
	if err := store.Load(strings.NewReader(jsonStream)); err != nil {
		t.Fatalf("failed to load single credential: %s", err.Error())
	}

	if store.AA("nonexistent", "password1", "foo") {
		t.Fatalf("nonexistent authenticated and authorized for foo")
	}

	if !store.AA("nonexistent", "password1", "qux") {
		t.Fatalf("nonexistent not authenticated and authorized for qux")
	}

	// explicit check of anonymous user
	if !store.AA("", "", "qux") {
		t.Fatalf("anonymous incorrectly not authorized")
	}
	if store.AA("", "", "foo") {
		t.Fatalf("anonymous incorrectly authorized")
	}

	if !store.AA("username1", "password1", "foo") {
		t.Fatalf("username1 not authenticated and authorized for foo")
	}
	if !store.AA("username1", "password1", "bar") {
		t.Fatalf("username1 not authenticated and authorized for bar")
	}
	if !store.AA("username1", "password1", "qux") {
		t.Fatalf("username1 not authenticated and authorized for qux")
	}
	if store.AA("username1", "password2", "bar") {
		t.Fatalf("username1 was authenticated and authorized for bar using wrong password")
	}
	if store.AA("username1", "password1", "quz") {
		t.Fatalf("username1 was authenticated and authorized for quz")
	}

	if !store.AA("username2", "password2", "baz") {
		t.Fatalf("username2 not authenticated and authorized for baz")
	}
	if store.AA("username2", "password1", "baz") {
		t.Fatalf("username2 authenticated and authorized for baz with wrong password")
	}
	if !store.AA("username2", "password2", "qux") {
		t.Fatalf("username2 not authenticated and authorized for qux")
	}
	if store.AA("username2", "password2", "bar") {
		t.Fatalf("username2 was authenticated and authorized for bar using wrong password")
	}
	if store.AA("username2", "password2", "quz") {
		t.Fatalf("username2 was authenticated and authorized for quz")
	}
}

func Test_AuthPermsRequestLoadSingle(t *testing.T) {
	const jsonStream = `
		[
			{
				"username": "username1",
				"password": "password1",
				"perms": ["foo", "bar"]
			}
		]
	`

	store := NewCredentialsStore()
	if err := store.Load(strings.NewReader(jsonStream)); err != nil {
		t.Fatalf("failed to load single credential: %s", err.Error())
	}

	if check := store.Check("username1", "password1"); !check {
		t.Fatalf("single credential not loaded correctly")
	}

	b1 := &testBasicAuther{
		username: "username1",
		password: "password1",
		ok:       true,
	}
	if perm := store.HasPermRequest(b1, "foo"); !perm {
		t.Fatalf("username1 does not have perm foo via request")
	}
	b2 := &testBasicAuther{
		username: "username2",
		password: "password1",
		ok:       true,
	}
	if perm := store.HasPermRequest(b2, "foo"); perm {
		t.Fatalf("username2 has perm foo via request")
	}
}

func Test_AuthPermsRequestLoadSingleFromFile(t *testing.T) {
	const jsonStream = `
		[
			{
				"username": "username1",
				"password": "password1",
				"perms": ["foo", "bar"]
			}
		]
	`
	path := mustWriteTempFile(t, jsonStream)
	defer fsutil.Remove(path)

	store, err := NewCredentialsStoreFromFile(path)
	if err != nil {
		t.Fatalf("failed to load credential store from file: %s", err.Error())
	}

	if check := store.Check("username1", "password1"); !check {
		t.Fatalf("single credential not loaded correctly")
	}

	b1 := &testBasicAuther{
		username: "username1",
		password: "password1",
		ok:       true,
	}
	if perm := store.HasPermRequest(b1, "foo"); !perm {
		t.Fatalf("username1 does not have perm foo via request")
	}
	b2 := &testBasicAuther{
		username: "username2",
		password: "password1",
		ok:       true,
	}
	if perm := store.HasPermRequest(b2, "foo"); perm {
		t.Fatalf("username2 has perm foo via request")
	}
}

func Test_AuthPermsAllUsers(t *testing.T) {
	const jsonStream = `
		[
			{
				"username": "username1",
				"password": "password1",
				"perms": ["foo"]
			},
			{
				"username": "*",
				"perms": ["bar", "abc"]
			}
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

	if perm := store.HasPerm("username1", "qux"); perm {
		t.Fatalf("username1 has qux perm")
	}
	if perm := store.HasPerm(AllUsers, "bar"); !perm {
		t.Fatalf("* does not have bar perm")
	}
	if perm := store.HasPerm(AllUsers, "abc"); !perm {
		t.Fatalf("* does not have abc perm")
	}
	if perm := store.HasPerm(AllUsers, "foo"); perm {
		t.Fatalf("* has foo perm")
	}
	if perm := store.HasPerm("username1", "bar"); !perm {
		t.Fatalf("username1 should have bar perm via *")
	}
	if perm := store.HasPerm("username1", "abc"); !perm {
		t.Fatalf("username1 should have abc perm via *")
	}
}

func mustWriteTempFile(t *testing.T, s string) string {
	f, err := os.CreateTemp(t.TempDir(), "rqlite-test")
	if err != nil {
		panic("failed to create temp file")
	}
	defer f.Close()
	if _, err := f.WriteString(s); err != nil {
		panic("failed to write to temp file")
	}
	return f.Name()
}
