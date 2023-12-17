package auth

import (
	"strings"
	"testing"
)

func BenchmarkCredentialStore(b *testing.B) {
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
		panic("failed to load multiple credentials")
	}

	b1 := &testBasicAuther{
		username: "username1",
		password: "password1",
		ok:       true,
	}

	for n := 0; n < b.N; n++ {
		store.CheckRequest(b1)
	}
}
