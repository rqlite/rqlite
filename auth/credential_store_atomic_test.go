package auth

import (
	"strings"
	"testing"
)

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
			input := `[{"username":"new","password":"new-secret","perms":["all"]},{"username":"*","perms":["all"]}` + suffix
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

func Test_AuthLoadAddsCredentials(t *testing.T) {
	store := NewCredentialsStore()
	for _, input := range []string{
		`[{"username":"first","password":"secret1","perms":["query"]}]`,
		`[{"username":"second","password":"secret2","perms":["execute"]}]`,
	} {
		if err := store.Load(strings.NewReader(input)); err != nil {
			t.Fatal(err)
		}
	}
	if !store.AA("first", "secret1", PermQuery) || !store.AA("second", "secret2", PermExecute) {
		t.Fatal("successful loads should retain both users")
	}
}
