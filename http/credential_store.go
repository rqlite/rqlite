package http

import (
	"encoding/json"
	"io"
)

type BasicAuther interface {
	BasicAuth() (string, string, bool)
}

type Credential struct {
	Username string   `json:"username,omitempty"`
	Password string   `json:"password,omitempty"`
	Perms    []string `json:"perms",omitempty"`
}

type CredentialsStore struct {
	store map[string]string
	perms map[string]map[string]bool
}

func NewCredentialsStore() *CredentialsStore {
	return &CredentialsStore{
		store: make(map[string]string),
		perms: make(map[string]map[string]bool),
	}
}

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

	// Read closing bracket
	_, err = dec.Token()
	if err != nil {
		return err
	}

	return nil
}

func (c *CredentialsStore) Check(username, password string) bool {
	pw, ok := c.store[username]
	return ok && password == pw
}

func (c *CredentialsStore) CheckRequest(b BasicAuther) bool {
	username, password, ok := b.BasicAuth()
	if !ok || !c.Check(username, password) {
		return false
	}
	return true
}

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
