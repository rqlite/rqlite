package auth

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

func Test_HashCache(t *testing.T) {
	hc := NewHashCache()

	if hc.Check("user", "hash1") {
		t.Fatalf("hash cache check OK for empty cache")
	}
	if hc.Check("user", "") {
		t.Fatalf("hash cache check OK for empty cache")
	}
	if hc.Check("", "") {
		t.Fatalf("hash cache check OK for empty cache")
	}

	hc.Store("user1", "hash1")
	if !hc.Check("user1", "hash1") {
		t.Fatalf("hash cache check not OK for user1")
	}
	if hc.Check("user", "hash1") {
		t.Fatalf("hash cache check OK for bad user")
	}

	hc.Store("user1", "hash2")
	if !hc.Check("user1", "hash1") {
		t.Fatalf("hash cache check not OK for user1")
	}
	if !hc.Check("user1", "hash2") {
		t.Fatalf("hash cache check not OK for user1")
	}

	hc.Store("user3", "hash3")
	if !hc.Check("user1", "hash1") {
		t.Fatalf("hash cache check not OK for user1")
	}
	if !hc.Check("user1", "hash2") {
		t.Fatalf("hash cache check not OK for user1")
	}
	if hc.Check("user", "hash1") {
		t.Fatalf("hash cache check OK for bad user")
	}
	if !hc.Check("user3", "hash3") {
		t.Fatalf("hash cache check not OK for user3")
	}
	if hc.Check("user3", "hash1") {
		t.Fatalf("hash cache check OK for user3, with bad hash")
	}
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
		t.Fatalf("username1 does not have baz perm")
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

func Test_AuthLoadHashedSingleRequest(t *testing.T) {
	const jsonStream = `
		[
			{
				"username": "username1",
				"password": "$2a$10$fKRHxrEuyDTP6tXIiDycr.nyC8Q7UMIfc31YMyXHDLgRDyhLK3VFS"
			},
			{	"username": "username2",
				"password": "password2"
			}
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
		username: "username2",
		password: "password2",
		ok:       true,
	}

	b3 := &testBasicAuther{
		username: "username1",
		password: "wrong",
		ok:       true,
	}
	b4 := &testBasicAuther{
		username: "username2",
		password: "wrong",
		ok:       true,
	}
	b5 := &testBasicAuther{
		username: "username1",
		password: "password2",
		ok:       true,
	}
	b6 := &testBasicAuther{
		username: "username2",
		password: "password1",
		ok:       true,
	}

	if check := store.CheckRequest(b1); !check {
		t.Fatalf("username1 (b1) credential not checked correctly via request")
	}
	if check := store.CheckRequest(b2); !check {
		t.Fatalf("username2 (b2) credential not checked correctly via request")
	}
	if check := store.CheckRequest(b3); check {
		t.Fatalf("username1 (b3) credential not checked correctly via request")
	}
	if check := store.CheckRequest(b4); check {
		t.Fatalf("username2 (b4) credential not checked correctly via request")
	}
	if check := store.CheckRequest(b5); check {
		t.Fatalf("username2 (b5) credential not checked correctly via request")
	}
	if check := store.CheckRequest(b6); check {
		t.Fatalf("username2 (b5) credential not checked correctly via request")
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
		t.Fatalf("username1 does not has perm foo via request")
	}
	b2 := &testBasicAuther{
		username: "username2",
		password: "password1",
		ok:       true,
	}
	if perm := store.HasPermRequest(b2, "foo"); perm {
		t.Fatalf("username1 does have perm foo via request")
	}

}

func Test_AuthPermsEmptyLoadSingle(t *testing.T) {
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
	if err := store.Load(strings.NewReader(jsonStream)); err != nil {
		t.Fatalf("failed to load single credential: %s", err.Error())
	}

	if check := store.Check("username1", "password1"); !check {
		t.Fatalf("single credential not loaded correctly")
	}
	if check := store.Check("username1", "wrong"); check {
		t.Fatalf("single credential not loaded correctly")
	}

	if perm := store.HasPerm("username1", "foo"); perm {
		t.Fatalf("wrong has foo perm")
	}
}

func Test_AuthPermsNilLoadSingle(t *testing.T) {
	const jsonStream = `
		[
			{
				"username": "username1",
				"password": "password1"
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

	if perm := store.HasPerm("username1", "foo"); perm {
		t.Fatalf("wrong has foo perm")
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
