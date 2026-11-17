package crypto

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"filippo.io/age"
)

func TestEncryptWriterDecryptReaderRoundTrip(t *testing.T) {
	identity := newTestIdentity(t)
	recipients := "# Active recipient\n\n" + identity.Recipient().String() + "\n"

	for _, size := range []int{0, 13, 64 << 10, (128 << 10) + 1} {
		t.Run(fmt.Sprintf("%d_bytes", size), func(t *testing.T) {
			plaintext := bytes.Repeat([]byte{0x00, 0x7f, 0x80, 0xff}, size/4+1)[:size]
			dst := &testWriter{}
			w, err := NewEncryptWriter(dst, recipients)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := io.Copy(w, bytes.NewReader(plaintext)); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if dst.closed {
				t.Fatal("closing the encrypting writer closed its destination")
			}

			got, err := decryptTestData(dst.Bytes(), identity.String())
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, plaintext) {
				t.Fatal("decrypted data does not match plaintext")
			}
		})
	}
}

func TestEncryptReaderDecryptReaderRoundTrip(t *testing.T) {
	identity := newTestIdentity(t)

	for _, size := range []int{0, 13, (128 << 10) + 1} {
		t.Run(fmt.Sprintf("%d_bytes", size), func(t *testing.T) {
			plaintext := bytes.Repeat([]byte{0x00, 0x7f, 0x80, 0xff}, size/4+1)[:size]
			r, err := NewEncryptReader(bytes.NewReader(plaintext), identity.Recipient().String())
			if err != nil {
				t.Fatal(err)
			}
			ciphertext, err := io.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}
			got, err := decryptTestData(ciphertext, identity.String())
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, plaintext) {
				t.Fatal("decrypted data does not match plaintext")
			}
		})
	}
}

func TestDecryptReaderMultipleIdentitiesRotation(t *testing.T) {
	oldIdentity, newIdentity := newTestIdentity(t), newTestIdentity(t)
	identities := "# Retained identity\n" + oldIdentity.String() +
		"\n\n# Current identity\n" + newIdentity.String() + "\n"
	plaintext := []byte("data retained across key rotation")

	for _, tc := range []struct {
		name     string
		identity *age.X25519Identity
	}{
		{"old_key", oldIdentity},
		{"new_key", newIdentity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ciphertext := encryptTestData(t, plaintext, tc.identity.Recipient())
			got, err := decryptTestData(ciphertext, identities)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, plaintext) {
				t.Fatal("decrypted data does not match plaintext")
			}
		})
	}
}

func TestDecryptReaderWrongIdentityAndTampering(t *testing.T) {
	identity, wrongIdentity := newTestIdentity(t), newTestIdentity(t)
	plaintext := bytes.Repeat([]byte("x"), (128<<10)+1)
	ciphertext := encryptTestData(t, plaintext, identity.Recipient())

	// Change a valid base64 character in the age header's MAC.
	badHeader := bytes.Clone(ciphertext)
	macOffset := bytes.Index(badHeader, []byte("\n--- "))
	if macOffset < 0 {
		t.Fatal("age fixture has no header MAC")
	}
	macOffset += len("\n--- ")
	if badHeader[macOffset] == 'A' {
		badHeader[macOffset] = 'B'
	} else {
		badHeader[macOffset] = 'A'
	}
	badPayload := bytes.Clone(ciphertext)
	badPayload[len(badPayload)-1] ^= 1

	var incomplete bytes.Buffer
	w, err := NewEncryptWriter(&incomplete, identity.Recipient().String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(w, bytes.NewReader(plaintext)); err != nil {
		t.Fatal(err)
	}
	// Intentionally omit Close: the stream must not be accepted without its
	// authenticated final chunk, even after complete earlier chunks were written.

	for _, tc := range []struct {
		name       string
		ciphertext []byte
		identities string
	}{
		{"wrong_identity", ciphertext, wrongIdentity.String()},
		{"header_mac", badHeader, identity.String()},
		{"payload", badPayload, identity.String()},
		{"truncated_byte", ciphertext[:len(ciphertext)-1], identity.String()},
		// The final age chunk contains one plaintext byte and a 16-byte tag.
		{"missing_final_chunk", ciphertext[:len(ciphertext)-17], identity.String()},
		{"unclosed_writer", incomplete.Bytes(), identity.String()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Earlier authenticated chunks may be returned; reading through the
			// end must report an error rather than a successful partial result.
			if _, err := decryptTestData(tc.ciphertext, tc.identities); err == nil {
				t.Fatal("expected decryption to fail")
			}
		})
	}
}

func TestValidationAndIOErrors(t *testing.T) {
	identity := newTestIdentity(t)
	recipient := identity.Recipient().String()
	privateKey := identity.String()
	plaintext := bytes.Repeat([]byte("x"), (128<<10)+1)
	ciphertext := encryptTestData(t, plaintext, identity.Recipient())

	t.Run("invalid_keys", func(t *testing.T) {
		for _, constructor := range []struct {
			name string
			new  func(string) error
		}{
			{"encrypt_writer", func(keys string) error {
				_, err := NewEncryptWriter(io.Discard, keys)
				return err
			}},
			{"encrypt_reader", func(keys string) error {
				_, err := NewEncryptReader(bytes.NewReader(plaintext), keys)
				return err
			}},
			{"decrypt_reader", func(keys string) error {
				_, err := NewDecryptReader(bytes.NewReader(ciphertext), keys)
				return err
			}},
		} {
			t.Run(constructor.name, func(t *testing.T) {
				for _, tc := range []struct {
					name string
					keys string
				}{
					{"empty", ""},
					{"comments_only", "# No keys\n\n"},
					{"malformed", "not-an-age-key"},
					{"private_key_redaction", privateKey + "!"},
				} {
					t.Run(tc.name, func(t *testing.T) {
						err := constructor.new(tc.keys)
						if err == nil {
							t.Fatal("expected invalid keys to be rejected")
						}
						if strings.Contains(err.Error(), privateKey) {
							t.Fatal("error contains private key material")
						}
					})
				}
			})
		}
	})

	for _, tc := range []struct {
		name string
		new  func() error
	}{
		{"nil_writer", func() error {
			_, err := NewEncryptWriter(nil, recipient)
			return err
		}},
		{"nil_encrypt_reader", func() error {
			_, err := NewEncryptReader(nil, recipient)
			return err
		}},
		{"nil_decrypt_reader", func() error {
			_, err := NewDecryptReader(nil, privateKey)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.new(); err == nil {
				t.Fatal("expected nil I/O argument to be rejected")
			}
		})
	}

	readErr := errors.New("test read failure")
	writeErr := errors.New("test write failure")

	t.Run("write_header", func(t *testing.T) {
		_, err := NewEncryptWriter(&testWriter{err: writeErr}, recipient)
		if !errors.Is(err, writeErr) {
			t.Fatalf("got %v, want %v", err, writeErr)
		}
	})

	t.Run("write_payload", func(t *testing.T) {
		dst := &testWriter{}
		w, err := NewEncryptWriter(dst, recipient)
		if err != nil {
			t.Fatal(err)
		}
		dst.err = writeErr
		if _, err := w.Write(plaintext); !errors.Is(err, writeErr) {
			t.Fatalf("got %v, want %v", err, writeErr)
		}
	})

	t.Run("write_final_chunk", func(t *testing.T) {
		dst := &testWriter{}
		w, err := NewEncryptWriter(dst, recipient)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte("final chunk")); err != nil {
			t.Fatal(err)
		}
		dst.err = writeErr
		if err := w.Close(); !errors.Is(err, writeErr) {
			t.Fatalf("got %v, want %v", err, writeErr)
		}
	})

	t.Run("read_plaintext", func(t *testing.T) {
		src := io.MultiReader(bytes.NewReader(plaintext), errorReader{readErr})
		r, err := NewEncryptReader(src, recipient)
		if err == nil {
			_, err = io.ReadAll(r)
		}
		if !errors.Is(err, readErr) {
			t.Fatalf("got %v, want %v", err, readErr)
		}
	})

	t.Run("read_header", func(t *testing.T) {
		_, err := NewDecryptReader(errorReader{readErr}, privateKey)
		if !errors.Is(err, readErr) {
			t.Fatalf("got %v, want %v", err, readErr)
		}
	})

	t.Run("read_payload", func(t *testing.T) {
		src := io.MultiReader(bytes.NewReader(ciphertext[:len(ciphertext)-1]), errorReader{readErr})
		r, err := NewDecryptReader(src, privateKey)
		if err == nil {
			_, err = io.ReadAll(r)
		}
		if !errors.Is(err, readErr) {
			t.Fatalf("got %v, want %v", err, readErr)
		}
	})
}

func newTestIdentity(t *testing.T) *age.X25519Identity {
	t.Helper()
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

// Use upstream age to generate valid inputs independently of our constructors.
func encryptTestData(t *testing.T, plaintext []byte, recipient age.Recipient) []byte {
	t.Helper()
	var dst bytes.Buffer
	w, err := age.Encrypt(&dst, recipient)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(w, bytes.NewReader(plaintext)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return dst.Bytes()
}

func decryptTestData(ciphertext []byte, identities string) ([]byte, error) {
	r, err := NewDecryptReader(bytes.NewReader(ciphertext), identities)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(r)
}

type testWriter struct {
	bytes.Buffer
	err    error
	closed bool
}

func (w *testWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	return w.Buffer.Write(p)
}

func (w *testWriter) Close() error {
	w.closed = true
	return nil
}

type errorReader struct {
	err error
}

func (r errorReader) Read([]byte) (int, error) {
	return 0, r.err
}
