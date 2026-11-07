// Package crypto provides streaming encryption and decryption in the age format.
package crypto

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"filippo.io/age"
)

// NewEncryptWriter returns a writer that encrypts plaintext written to it and
// writes the age-encrypted stream to dst. Recipients are newline-separated age
// public keys; blank lines and lines starting with "#" are ignored.
//
// The caller must call Close to flush and authenticate the final chunk. Close
// does not close dst. The caller must check both plaintext-copy errors and the
// Close error before accepting the encrypted stream as complete.
func NewEncryptWriter(dst io.Writer, recipients string) (io.WriteCloser, error) {
	if dst == nil {
		return nil, errors.New("nil destination writer")
	}
	parsed, err := age.ParseRecipients(strings.NewReader(recipients))
	if err != nil {
		return nil, fmt.Errorf("failed to parse age recipients: %w", err)
	}
	return age.Encrypt(dst, parsed...)
}

// NewEncryptReader returns a reader that reads plaintext from src and produces
// an age-encrypted stream. Recipients are newline-separated age public keys;
// blank lines and lines starting with "#" are ignored.
//
// The caller must read through EOF and check for errors to obtain a complete
// encrypted stream. The returned reader does not close src.
func NewEncryptReader(src io.Reader, recipients string) (io.Reader, error) {
	if src == nil {
		return nil, errors.New("nil source reader")
	}
	parsed, err := age.ParseRecipients(strings.NewReader(recipients))
	if err != nil {
		return nil, fmt.Errorf("failed to parse age recipients: %w", err)
	}
	return age.EncryptReader(src, parsed...)
}

// NewDecryptReader returns a reader that decrypts an age-encrypted stream from
// src. Identities are newline-separated age private keys; blank lines and lines
// starting with "#" are ignored. Age tries the identities until one matches.
//
// Plaintext is authenticated chunk by chunk. The caller must read through EOF
// and check for errors before accepting the entire stream as complete and valid.
// The returned reader does not close src.
func NewDecryptReader(src io.Reader, identities string) (io.Reader, error) {
	if src == nil {
		return nil, errors.New("nil source reader")
	}
	parsed, err := age.ParseIdentities(strings.NewReader(identities))
	if err != nil {
		return nil, fmt.Errorf("failed to parse age identities: %w", err)
	}
	return age.Decrypt(src, parsed...)
}
