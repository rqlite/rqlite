package restore

import (
	"encoding/json"
	"io"
	"os"

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

// NewStorageClient unmarshals the config data and returns the Config and StorageClient.
func NewStorageClient(data []byte) (*Config, StorageClient, error) {
	dCfg := &Config{}
	err := json.Unmarshal(data, dCfg)
	if err != nil {
		return nil, nil, err
	}

	if dCfg.Version > auto.Version {
		return nil, nil, auto.ErrInvalidVersion
	}

	var sc StorageClient
	switch dCfg.Type {
	case auto.StorageTypeS3:
		s3cfg := &aws.S3Config{}
		err = json.Unmarshal(dCfg.Sub, s3cfg)
		if err != nil {
			return nil, nil, err
		}
		s3ClientOps := &aws.S3ClientOpts{
			ForcePathStyle: s3cfg.ForcePathStyle,
		}
		sc, err = aws.NewS3Client(s3cfg.Endpoint, s3cfg.Region, s3cfg.AccessKeyID, s3cfg.SecretAccessKey,
			s3cfg.Bucket, s3cfg.Path, s3ClientOps)
	case auto.StorageTypeGCS:
		gcsCfg := &gcp.GCSConfig{}
		err = json.Unmarshal(dCfg.Sub, gcsCfg)
		if err != nil {
			return nil, nil, err
		}
		sc, err = gcp.NewGCSClient(gcsCfg)
	default:
		return nil, nil, auto.ErrUnsupportedStorageType
	}

	return dCfg, sc, err
}
