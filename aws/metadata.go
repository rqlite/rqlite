package aws

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

type MetadataClient struct {
	client *http.Client
	URL    string
}

func NewMetadataClient() *MetadataClient {
	return &MetadataClient{
		client: &http.Client{},
		URL:    `http://169.254.169.254/`,
	}
}

func (m *MetadataClient) LocalIPv4() (string, error) {
	return m.get("/latest/meta-data/local-ipv4")
}

func (m *MetadataClient) PublicIPv4() (string, error) {
	return m.get("/latest/meta-data/public-ipv4")
}

func (m *MetadataClient) get(path string) (string, error) {
	resp, err := m.client.Get(m.URL + path)
	if err != nil {
		return "", err
	}

	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to request %s, got: %s", path, resp.Status)
	}
	return string(b), nil
}
