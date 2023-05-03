package download

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"time"

	"github.com/rqlite/rqlite/aws"
)

const (
	// version is the max version of the config file format supported
	version = 1
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

// Config is the config file format for the upload service
type Config struct {
	Version int             `json:"version"`
	Type    StorageType     `json:"type"`
	Timeout Duration        `json:"timeout"`
	Sub     json.RawMessage `json:"sub"`
}

// Unmarshal unmarshals the config file and returns the config and subconfig
func Unmarshal(data []byte) (*Config, *aws.S3Config, error) {
	cfg := &Config{}
	err := json.Unmarshal(data, cfg)
	if err != nil {
		return nil, nil, err
	}

	if cfg.Version > version {
		return nil, nil, ErrInvalidVersion
	}

	s3cfg := &aws.S3Config{}
	err = json.Unmarshal(cfg.Sub, s3cfg)
	if err != nil {
		return nil, nil, err
	}
	return cfg, s3cfg, nil
}

// ReadConfigFile reads the config file and returns the data. It also expands
// any environment variables in the config file.
func ReadConfigFile(filename string) ([]byte, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	data, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	data = []byte(os.ExpandEnv(string(data)))
	return data, nil
}
