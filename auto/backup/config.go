package backup

import (
	"encoding/json"
	"io"
	"os"

	"github.com/rqlite/rqlite/v8/auto"
	"github.com/rqlite/rqlite/v8/auto/aws"
	"github.com/rqlite/rqlite/v8/auto/gcp"
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

// NewStorageClient unmarshals the config data and returns the Config and StorageClient.
func NewStorageClient(data []byte) (*Config, StorageClient, error) {
	cfg := &Config{}
	err := json.Unmarshal(data, cfg)
	if err != nil {
		return nil, nil, err
	}

	if cfg.Version > auto.Version {
		return nil, nil, auto.ErrInvalidVersion
	}

	var sc StorageClient
	switch cfg.Type {
	case auto.StorageTypeS3:
		s3cfg := &aws.S3Config{}
		err = json.Unmarshal(cfg.Sub, s3cfg)
		if err != nil {
			return nil, nil, err
		}
		opts := &aws.S3ClientOpts{
			ForcePathStyle: s3cfg.ForcePathStyle,
			Timestamp:      cfg.Timestamp,
		}
		sc, err = aws.NewS3Client(s3cfg.Endpoint, s3cfg.Region, s3cfg.AccessKeyID, s3cfg.SecretAccessKey,
			s3cfg.Bucket, s3cfg.Path, opts)
	case auto.StorageTypeGCS:
		gcsCfg := &gcp.GCSConfig{}
		err = json.Unmarshal(cfg.Sub, gcsCfg)
		if err != nil {
			return nil, nil, err
		}
		opts := &gcp.GCSClientOpts{
			Timestamp: cfg.Timestamp,
		}
		sc, err = gcp.NewGCSClient(gcsCfg, opts)
	default:
		return nil, nil, auto.ErrUnsupportedStorageType
	}

	return cfg, sc, err
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
