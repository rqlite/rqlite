package download

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"time"

	"github.com/rqlite/rqlite/autostate"
	"github.com/rqlite/rqlite/aws"
)

// Config is the config file format for the upload service
type Config struct {
	Version           int                   `json:"version"`
	Type              autostate.StorageType `json:"type"`
	Timeout           autostate.Duration    `json:"timeout,omitempty"`
	ContinueOnFailure bool                  `json:"continue_on_failure,omitempty"`
	Sub               json.RawMessage       `json:"sub"`
}

// Unmarshal unmarshals the config file and returns the config and subconfig
func Unmarshal(data []byte) (*Config, *aws.S3Config, error) {
	cfg := &Config{}
	err := json.Unmarshal(data, cfg)
	if err != nil {
		return nil, nil, err
	}

	if cfg.Version > autostate.Version {
		return nil, nil, autostate.ErrInvalidVersion
	}

	if cfg.Timeout == 0 {
		cfg.Timeout = autostate.Duration(30 * time.Second)
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
