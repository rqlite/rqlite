package regexp

import (
	"encoding/json"
	"fmt"
	"regexp"
)

// Regexp is a JSON-friendly wrapper around *regexp.Regexp.
// JSON representation: a string with the pattern (e.g. "(?i)^foo$") or null.
type Regexp struct {
	*regexp.Regexp
}

// Compile compiles s into a Regexp.
func Compile(s string) (Regexp, error) {
	if s == "" {
		return Regexp{}, nil
	}
	rx, err := regexp.Compile(s)
	if err != nil {
		return Regexp{}, err
	}
	return Regexp{Regexp: rx}, nil
}

// MustCompile compiles s or panics.
func MustCompile(s string) Regexp {
	rx, err := Compile(s)
	if err != nil {
		panic(err)
	}
	return rx
}

// String returns the pattern or "" if nil.
func (r Regexp) String() string {
	if r.Regexp == nil {
		return ""
	}
	return r.Regexp.String()
}

// IsZero reports whether the value should be considered empty.
func (r Regexp) IsZero() bool { return r.Regexp == nil }

// MarshalJSON encodes the pattern as a JSON string, or null if unset.
func (r Regexp) MarshalJSON() ([]byte, error) {
	if r.Regexp == nil {
		return []byte("null"), nil
	}
	return json.Marshal(r.Regexp.String())
}

// UnmarshalJSON accepts a JSON string pattern or null.
// An empty string sets the value to nil.
func (r *Regexp) UnmarshalJSON(b []byte) error {
	var s *string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("regexp must be a JSON string or null: %w", err)
	}

	if s == nil || *s == "" {
		r.Regexp = nil
		return nil
	}

	rr, err := regexp.Compile(*s)
	if err != nil {
		return fmt.Errorf("invalid regexp %q: %w", *s, err)
	}
	r.Regexp = rr
	return nil
}
