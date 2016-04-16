package http

import (
	"encoding/json"
	"io"
)

type BasicAuther interface {
	BasicAuth() (string, string, bool)
}

type Credential struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

type CredentialsStore struct {
	store map[string]string
}

func NewCredentialsStore() *CredentialsStore {
	return &CredentialsStore{
		store: make(map[string]string),
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
