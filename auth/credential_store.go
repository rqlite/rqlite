// Package auth is a lightweight credential store.
// It provides functionality for loading credentials, as well as validating credentials.
package auth

import (
	"encoding/json"
	"io"

	"golang.org/x/crypto/bcrypt"
)

// BasicAuther is the interface an object must support to return basic auth information.
type BasicAuther interface {
	BasicAuth() (string, string, bool)
}

// Credential represents authentication and authorization configuration for a single user.
type Credential struct {
	Username string   `json:"username,omitempty"`
	Password string   `json:"password,omitempty"`
	Perms    []string `json:"perms,omitempty"`
}

// CredentialsStore stores authentication and authorization information for all users.
type CredentialsStore struct {
	store map[string]string
	perms map[string]map[string]bool
}

// NewCredentialsStore returns a new instance of a CredentialStore.
func NewCredentialsStore() *CredentialsStore {
	return &CredentialsStore{
		store: make(map[string]string),
		perms: make(map[string]map[string]bool),
	}
}

// Load loads credential information from a reader.
func (c *CredentialsStore) Load(r io.Reader) error {
	dec := json.NewDecoder(r)
	// Read open bracket
	_, err := dec.Token()
	if err != nil {
		return err
	}

	var cred Credential
	for dec.More() {
		err := dec.Decode(&cred)
		if err != nil {
			return err
		}
		c.store[cred.Username] = cred.Password
		c.perms[cred.Username] = make(map[string]bool, len(cred.Perms))
		for _, p := range cred.Perms {
			c.perms[cred.Username][p] = true
		}
	}

	// Read closing bracket.
	_, err = dec.Token()
	if err != nil {
		return err
	}

	return nil
}

// Check returns true if the password is correct for the given username.
func (c *CredentialsStore) Check(username, password string) bool {
	pw, ok := c.store[username]
	if !ok {
		return false
	}
	return password == pw ||
		bcrypt.CompareHashAndPassword([]byte(pw), []byte(password)) == nil
}

// CheckRequest returns true if b contains a valid username and password.
func (c *CredentialsStore) CheckRequest(b BasicAuther) bool {
	username, password, ok := b.BasicAuth()
	if !ok || !c.Check(username, password) {
		return false
	}
	return true
}

// HasPerm returns true if username has the given perm. It does not
// perform any password checking.
func (c *CredentialsStore) HasPerm(username string, perm string) bool {
	m, ok := c.perms[username]
	if !ok {
		return false
	}

	if _, ok := m[perm]; !ok {
		return false
	}
	return true
}

// HasAnyPerm returns true if username has at least one of the given perms.
// It does not perform any password checking.
func (c *CredentialsStore) HasAnyPerm(username string, perm ...string) bool {
	return func(p []string) bool {
		for i := range p {
			if c.HasPerm(username, p[i]) {
				return true
			}
		}
		return false
	}(perm)
}

// HasPermRequest returns true if the username returned by b has the givem perm.
// It does not perform any password checking, but if there is no username
// in the request, it returns false.
func (c *CredentialsStore) HasPermRequest(b BasicAuther, perm string) bool {
	username, _, ok := b.BasicAuth()
	if !ok {
		return false
	}
	return c.HasPerm(username, perm)
}
