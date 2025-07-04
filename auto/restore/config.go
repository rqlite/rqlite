package restore

import (
	"encoding/json"
	"io"
	"os"
	"time"

	"github.com/rqlite/rqlite/v8/auto"
	"github.com/rqlite/rqlite/v8/aws"
	"github.com/rqlite/rqlite/v8/gcp"
)

// Config is the config file format for the upload service
type Config struct {
	Version           int              `json:"version"`
	Type              auto.StorageType `json:"type"`
	Timeout           auto.Duration    `json:"timeout,omitempty"`
	ContinueOnFailure bool             `json:"continue_on_failure,omitempty"`
	Sub               json.RawMessage  `json:"sub"`
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

	if cfg.Timeout == 0 {
		cfg.Timeout = auto.Duration(30 * time.Second)
	}

	s3cfg := &aws.S3Config{}
	err = json.Unmarshal(cfg.Sub, s3cfg)
	if err != nil {
		return nil, nil, err
	}
	return cfg, s3cfg, nil
}

// UnmarshalGCS unmarshals the config file and returns the config and GCS subconfig
func UnmarshalGCS(data []byte) (*Config, *gcp.GCSConfig, error) {
	cfg := &Config{}
	err := json.Unmarshal(data, cfg)
	if err != nil {
		return nil, nil, err
	}

	if cfg.Version > auto.Version {
		return nil, nil, auto.ErrInvalidVersion
	}

	if cfg.Timeout == 0 {
		cfg.Timeout = auto.Duration(30 * time.Second)
	}

	gcsCfg := &gcp.GCSConfig{}
	err = json.Unmarshal(cfg.Sub, gcsCfg)
	if err != nil {
		return nil, nil, err
	}
	return cfg, gcsCfg, nil
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
