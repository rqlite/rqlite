package backup

import (
	"encoding/json"
	"io"
	"os"

	"github.com/rqlite/rqlite/v8/auto"
	"github.com/rqlite/rqlite/v8/aws"
)

// Config is the config file format for the upload service
type Config struct {
	Version    int              `json:"version"`
	Type       auto.StorageType `json:"type"`
	NoCompress bool             `json:"no_compress,omitempty"`
	Timestamp  bool             `json:"timestamp"`
	Vacuum     bool             `json:"vacuum,omitempty"`
	Interval   auto.Duration    `json:"interval"`
	Sub        json.RawMessage  `json:"sub"`
}

// Unmarshal unmarshals the config file and returns the config and subconfig
func Unmarshal(data []byte) (*Config, *aws.S3Config, error) {
	cfg := &Config{}
	err := json.Unmarshal(data, cfg)
	if err != nil {
		return nil, nil, err
	}

	if cfg.Version > auto.Version {
		return nil, nil, auto.ErrInvalidVersion
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

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	data = []byte(os.ExpandEnv(string(data)))
	return data, nil
}
