package gcp

import (
	"bytes"
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rqlite/rqlite/v8/gcp/jws"
)

var (
	defaultEndpoint         = "https://storage.googleapis.com"
	defaultDownloadBufferSz = 32 * 1024 // 32 KiB
	jwtScope                = "https://www.googleapis.com/auth/devstorage.read_write"
	jwtAudTarget            = "https://oauth2.googleapis.com/token"

	GCPGCSIDKey = "rqlite-auto-backup-id"
)

// TimestampedPath returns a new path with the given timestamp prepended.
// If path contains /, the timestamp is prepended to the last segment.
func TimestampedPath(path string, t time.Time) string {
	parts := strings.Split(path, "/")
	parts[len(parts)-1] = fmt.Sprintf("%s_%s", t.Format("20060102150405"), parts[len(parts)-1])
	return strings.Join(parts, "/")
}

// GCSConfig is the subconfig for the GCS storage type.
type GCSConfig struct {
	Endpoint        string `json:"endpoint,omitempty"`
	ProjectID       string `json:"project_id"`
	Bucket          string `json:"bucket"`
	Name            string `json:"name"`
	CredentialsPath string `json:"credentials_path"`
}

// GCSClient is a client for uploading data to Google Cloud Storage (GCS).
type GCSClient struct {
	cfg *GCSConfig

	sa          serviceAccount
	accessToken string
	expiry      time.Time
	tokenMu     sync.Mutex

	http      *http.Client
	uploadURL string
	objectURL string
	bucketURL string

	timestamp bool
}

// GCSClientOpts are options for creating a GCSClient.
type GCSClientOpts struct {
	Timestamp bool
}

// NewGCSClient returns an instance of a GCSClient.
func NewGCSClient(cfg *GCSConfig, opts *GCSClientOpts) (*GCSClient, error) {
	if cfg.Endpoint == "" {
		cfg.Endpoint = defaultEndpoint
	}
	sa, err := loadServiceAccount(cfg.CredentialsPath)
	if err != nil {
		return nil, err
	}
	base := strings.TrimRight(cfg.Endpoint, "/")

	return &GCSClient{
		cfg:       cfg,
		sa:        *sa,
		http:      &http.Client{},
		uploadURL: fmt.Sprintf("%s/upload/storage/v1/b/%s/o", base, url.PathEscape(cfg.Bucket)),
		objectURL: fmt.Sprintf("%s/storage/v1/b/%s/o/%s",
			base, url.PathEscape(cfg.Bucket), url.PathEscape(cfg.Name)),
		bucketURL: fmt.Sprintf("%s/storage/v1/b/%s", base, url.PathEscape(cfg.Bucket)),
		timestamp: opts != nil && opts.Timestamp,
	}, nil
}

// String returns a string representation of the GCSClient.
func (g *GCSClient) String() string {
	return fmt.Sprintf("gs://%s/%s", g.cfg.Bucket, g.cfg.Name)
}

// EnsureBucket ensures the bucket actually exists in GCS.
func (g *GCSClient) EnsureBucket(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.bucketURL, nil)
	if err != nil {
		return err
	}
	if err := g.addAuth(req); err != nil {
		return err
	}
	res, err := g.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	switch res.StatusCode {
	case http.StatusOK:
		return nil // already exists
	case http.StatusNotFound:
		body := fmt.Sprintf(`{"name":"%s"}`, g.cfg.Bucket)
		u := g.bucketURL + "?project=" + url.QueryEscape(g.cfg.ProjectID)
		req, _ = http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if err := g.addAuth(req); err != nil {
			return err
		}
		res, err = g.http.Do(req)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(res.Body)
			return fmt.Errorf("bucket creation failed: %s", b)
		}
		return nil
	default:
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("bucket check failed: %s", b)
	}
}

// Upload uploads data to GCS.
func (g *GCSClient) Upload(ctx context.Context, r io.Reader, id string) error {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)

	name := g.cfg.Name
	if g.timestamp {
		name = TimestampedPath(name, time.Now().UTC())
	}
	metaData := struct {
		Name     string `json:"name"`
		Metadata struct {
			ID string `json:"rqlite-auto-backup-id"`
		} `json:"metadata"`
	}{
		Name: name,
	}
	metaData.Metadata.ID = id

	meta, err := json.Marshal(metaData)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	hdr := textproto.MIMEHeader{"Content-Type": {"application/json"}}
	part, err := w.CreatePart(hdr)
	if err != nil {
		return fmt.Errorf("failed to create metadata part: %w", err)
	}
	if _, err := part.Write(meta); err != nil {
		return fmt.Errorf("failed to write metadata part: %w", err)
	}

	hdr = textproto.MIMEHeader{"Content-Type": {"application/octet-stream"}}
	part, err = w.CreatePart(hdr)
	if err != nil {
		return fmt.Errorf("failed to create data part: %w", err)
	}
	if _, err := io.Copy(part, r); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to close multipart writer: %w", err)
	}

	u := g.uploadURL + "?uploadType=multipart"
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, u, &buf)
	req.Header.Set("Content-Type", "multipart/related; boundary="+w.Boundary())
	if err := g.addAuth(req); err != nil {
		return err
	}

	res, err := g.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("upload failed: %s", b)
	}
	return nil
}

// Download downloads data from GCS.
func (g *GCSClient) Download(ctx context.Context, w io.WriterAt) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, g.objectURL+"?alt=media", nil)
	if err := g.addAuth(req); err != nil {
		return err
	}
	res, err := g.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("download failed: %s", b)
	}

	buf := make([]byte, defaultDownloadBufferSz)
	var off int64
	for {
		n, err := res.Body.Read(buf)
		if n > 0 {
			if _, werr := w.WriteAt(buf[:n], off); werr != nil {
				return werr
			}
			off += int64(n)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
	}
	return nil
}

// Delete deletes object from GCS.
func (g *GCSClient) Delete(ctx context.Context) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodDelete, g.objectURL, nil)
	if err := g.addAuth(req); err != nil {
		return err
	}
	res, err := g.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	switch res.StatusCode {
	case http.StatusOK, http.StatusNoContent, http.StatusNotFound:
		return nil
	default:
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("delete failed: %s", b)
	}
}

// CurrentID returns the last ID uploaded to GCS.
func (g *GCSClient) CurrentID(ctx context.Context) (string, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, g.objectURL, nil)
	if err := g.addAuth(req); err != nil {
		return "", err
	}
	res, err := g.http.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		return "", fmt.Errorf("metadata fetch failed: %s", b)
	}
	var obj struct {
		Metadata map[string]string `json:"metadata"`
	}
	if err := json.NewDecoder(res.Body).Decode(&obj); err != nil {
		return "", err
	}

	id, ok := obj.Metadata[GCPGCSIDKey]
	if !ok {
		return "", fmt.Errorf("sum metadata not found for %v", g)
	}
	return id, nil
}

func (g *GCSClient) addAuth(req *http.Request) error {
	tok, err := g.getToken(req.Context())
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	return nil
}

func (g *GCSClient) getToken(ctx context.Context) (string, error) {
	g.tokenMu.Lock()
	defer g.tokenMu.Unlock()

	if time.Until(g.expiry) > 2*time.Minute {
		return g.accessToken, nil
	}

	jwt, err := makeJWT(&g.sa)
	if err != nil {
		return "", err
	}
	tok, exp, err := fetchToken(ctx, jwt, g.http)
	if err != nil {
		return "", err
	}
	g.accessToken, g.expiry = tok, exp
	return tok, nil
}

func makeJWT(sa *serviceAccount) (string, error) {
	priv, err := parseKey(sa.PrivateKey)
	if err != nil {
		return "", err
	}
	now := time.Now().Unix()
	cs := &jws.ClaimSet{
		Iss:   sa.ClientEmail,
		Scope: jwtScope,
		Aud:   jwtAudTarget,
		Iat:   now,
		Exp:   now + 3600,
	}
	hdr := &jws.Header{Algorithm: "RS256", Typ: "JWT"}
	return jws.Encode(hdr, cs, priv)
}

type serviceAccount struct {
	ClientEmail string `json:"client_email"`
	PrivateKey  string `json:"private_key"`
}

func loadServiceAccount(p string) (*serviceAccount, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var sa serviceAccount
	if err := json.NewDecoder(f).Decode(&sa); err != nil {
		return nil, err
	}
	return &sa, nil
}

func parseKey(pemKey string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(pemKey))
	if block == nil {
		return nil, fmt.Errorf("invalid PEM")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("not RSA")
	}
	return rsaKey, nil
}

type tokenResp struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
}

func fetchToken(ctx context.Context, jwt string, hc *http.Client) (string, time.Time, error) {
	data := url.Values{
		"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"},
		"assertion":  {jwt},
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		jwtAudTarget, strings.NewReader(data.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	res, err := hc.Do(req)
	if err != nil {
		return "", time.Time{}, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		return "", time.Time{}, fmt.Errorf("token exchange failed: %s", b)
	}
	var tr tokenResp
	if err := json.NewDecoder(res.Body).Decode(&tr); err != nil {
		return "", time.Time{}, err
	}
	exp := time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second)
	return tr.AccessToken, exp, nil
}
