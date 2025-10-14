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

	// ErrInvalidInterval is returned when the interval is invalid.
	ErrInvalidInterval = errors.New("invalid interval")
)

// Duration is a wrapper around time.Duration that allows us to unmarshal
type Duration time.Duration

// MarshalJSON marshals the duration as a string
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// UnmarshalJSON unmarshals the duration from a string or a float64
func (d *Duration) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		*d = Duration(time.Duration(value))
	case string:
		tmp, err := time.ParseDuration(value)
		if err != nil {
			return err
		}
		*d = Duration(tmp)
	default:
		return errors.New("invalid duration")
	}
	if *d <= 0 {
		return ErrInvalidInterval
	}
	return nil
}

// StorageType represents the type of storage service used for backups.
type StorageType string

const (
	// StorageTypeS3 is the storage type for Amazon S3
	StorageTypeS3 StorageType = "s3"

	// StorageTypeGCS is the storage type for Google Cloud Storage
	StorageTypeGCS StorageType = "gcs"

	// StorageTypeFile is the storage type for local file storage
	StorageTypeFile StorageType = "file"
)

// UnmarshalJSON unmarshals the storage type from a string and validates it
func (s *StorageType) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case string:
		*s = StorageType(value)
		if *s != StorageTypeS3 && *s != StorageTypeGCS && *s != StorageTypeFile {
			return ErrUnsupportedStorageType
		}
		return nil
	default:
		return ErrUnsupportedStorageType
	}
}
