package lookup

import (
	"encoding/json"
	"io"
)

type Config struct {
	Name string `json:"name,omitempty"`
	Port int    `json:"port",omitempty`
}

func NewConfigFromReader(r io.Reader) (*Config, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
