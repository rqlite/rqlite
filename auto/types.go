package auto

import (
	"encoding/json"
	"errors"
	"time"
)

const (
	// Version is the max version of the config file format supported
	Version = 1
)

var (
	// ErrInvalidVersion is returned when the config file version is not supported.
	ErrInvalidVersion = errors.New("invalid version")

	// ErrUnsupportedStorageType is returned when the storage type is not supported.
	ErrUnsupportedStorageType = errors.New("unsupported storage type")
)

// Duration is a wrapper around time.Duration that allows us to unmarshal
type Duration time.Duration

// MarshalJSON marshals the duration as a string
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// UnmarshalJSON unmarshals the duration from a string or a float64
func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		*d = Duration(time.Duration(value))
		return nil
	case string:
		tmp, err := time.ParseDuration(value)
		if err != nil {
			return err
		}
		*d = Duration(tmp)
		return nil
	default:
		return errors.New("invalid duration")
	}
}

// StorageType is a wrapper around string that allows us to unmarshal
type StorageType string

// UnmarshalJSON unmarshals the storage type from a string and validates it
func (s *StorageType) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case string:
		*s = StorageType(value)
		if *s != "s3" {
			return ErrUnsupportedStorageType
		}
		return nil
	default:
		return ErrUnsupportedStorageType
	}
}
