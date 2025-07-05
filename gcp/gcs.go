package gcp

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
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
)

// Config holds the parameters needed to build a GCSClient.
type Config struct {
	UploadEndpoint string // e.g. "https://storage.googleapis.com"
	Bucket         string
	ProjectID      string
	ObjectName     string
	CredentialPath string
}

// GCSClient performs one-shot operations against a single bucket / object.
type GCSClient struct {
	cfg Config

	sa          serviceAccount
	tok         string
	tokExpiry   time.Time
	tokenMu     sync.Mutex
	httpClient  *http.Client
	uploadURL   string
	objectURL   string
	bucketURL   string
	metadataURL string
}

// New returns a ready-to-use client.
func New(cfg Config) (*GCSClient, error) {
	if cfg.UploadEndpoint == "" {
		cfg.UploadEndpoint = "https://storage.googleapis.com"
	}

	sa, err := loadServiceAccount(cfg.CredentialPath)
	if err != nil {
		return nil, err
	}

	base := strings.TrimRight(cfg.UploadEndpoint, "/")
	upURL := fmt.Sprintf("%s/upload/storage/v1/b/%s/o", base, url.PathEscape(cfg.Bucket))
	objURL := fmt.Sprintf("%s/storage/v1/b/%s/o/%s",
		base, url.PathEscape(cfg.Bucket), url.PathEscape(cfg.ObjectName))
	bktURL := fmt.Sprintf("%s/storage/v1/b/%s", base, url.PathEscape(cfg.Bucket))

	return &GCSClient{
		cfg:         cfg,
		sa:          *sa,
		httpClient:  http.DefaultClient,
		uploadURL:   upURL,
		objectURL:   objURL,
		bucketURL:   bktURL,
		metadataURL: objURL, // same URL, no alt=media
	}, nil
}

// EnsureBucket creates the bucket if it does not exist.
func (c *GCSClient) EnsureBucket(ctx context.Context) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.bucketURL, nil)
	if err := c.addAuth(req); err != nil {
		return err
	}

	res, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusOK {
		return nil // already exists
	}
	if res.StatusCode != http.StatusNotFound {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("bucket check failed: %s", b)
	}

	// Need to create.
	body := fmt.Sprintf(`{"name":"%s"}`, c.cfg.Bucket)
	u := fmt.Sprintf("%s?project=%s", c.bucketURL, url.QueryEscape(c.cfg.ProjectID))
	req, _ = http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if err := c.addAuth(req); err != nil {
		return err
	}

	res, err = c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("bucket creation failed: %s", b)
	}
	return nil
}

// Upload streams reader into the object and sets its custom ID metadata.
func (c *GCSClient) Upload(ctx context.Context, r io.Reader, id string) error {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)

	// Part 1: object metadata.
	meta := fmt.Sprintf(`{"name":"%s","metadata":{"id":"%s"}}`, c.cfg.ObjectName, id)
	hdr := textproto.MIMEHeader{"Content-Type": {"application/json; charset=UTF-8"}}
	part, _ := mw.CreatePart(hdr)
	part.Write([]byte(meta))

	// Part 2: the data.
	hdr = textproto.MIMEHeader{"Content-Type": {"application/octet-stream"}}
	part, _ = mw.CreatePart(hdr)
	if _, err := io.Copy(part, r); err != nil {
		return err
	}
	mw.Close()

	u := c.uploadURL + "?uploadType=multipart"
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, u, &buf)
	req.Header.Set("Content-Type", "multipart/related; boundary="+mw.Boundary())
	if err := c.addAuth(req); err != nil {
		return err
	}

	res, err := c.httpClient.Do(req)
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

// Download writes the object into writer, starting at offset 0.
func (c *GCSClient) Download(ctx context.Context, w io.WriterAt) error {
	u := c.objectURL + "?alt=media"
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err := c.addAuth(req); err != nil {
		return err
	}

	res, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("download failed: %s", b)
	}

	var (
		buf     = make([]byte, 32*1024)
		offset  int64
		werr    error
		readErr error
	)
	for {
		n, err := res.Body.Read(buf)
		if n > 0 {
			if _, werr = w.WriteAt(buf[:n], offset); werr != nil {
				return werr
			}
			offset += int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			readErr = err
			break
		}
	}
	return readErr
}

// Delete removes the object.
func (c *GCSClient) Delete(ctx context.Context) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodDelete, c.objectURL, nil)
	if err := c.addAuth(req); err != nil {
		return err
	}

	res, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		return nil // already gone
	}
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("delete failed: %s", b)
	}
	return nil
}

// CurrentID reads the custom metadata "id" of the current object.
func (c *GCSClient) CurrentID(ctx context.Context) (string, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.metadataURL, nil)
	if err := c.addAuth(req); err != nil {
		return "", err
	}

	res, err := c.httpClient.Do(req)
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
	return obj.Metadata["id"], nil
}

func (c *GCSClient) addAuth(req *http.Request) error {
	tok, err := c.token(req.Context())
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	return nil
}

func (c *GCSClient) token(ctx context.Context) (string, error) {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()

	if time.Until(c.tokExpiry) > 2*time.Minute {
		return c.tok, nil
	}

	jwt, err := jwtSigned(&c.sa,
		"https://www.googleapis.com/auth/devstorage.read_write",
		time.Now(),
	)
	if err != nil {
		return "", err
	}

	tok, exp, err := fetchAccessToken(ctx, jwt, c.httpClient)
	if err != nil {
		return "", err
	}
	c.tok, c.tokExpiry = tok, exp
	return tok, nil
}

type serviceAccount struct {
	ClientEmail string `json:"client_email"`
	PrivateKey  string `json:"private_key"`
}

func loadServiceAccount(path string) (*serviceAccount, error) {
	f, err := os.Open(path)
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

func parsePrivateKey(pemKey string) (*rsa.PrivateKey, error) {
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

func jwtSigned(sa *serviceAccount, scope string, now time.Time) (string, error) {
	priv, err := parsePrivateKey(sa.PrivateKey)
	if err != nil {
		return "", err
	}

	hdr := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	iat := now.Unix()
	exp := iat + 3600
	claims := fmt.Sprintf(`{"iss":"%s","scope":"%s","aud":"https://oauth2.googleapis.com/token","iat":%d,"exp":%d}`,
		sa.ClientEmail, scope, iat, exp)
	clm := base64.RawURLEncoding.EncodeToString([]byte(claims))

	unsigned := hdr + "." + clm
	hash := sha256.Sum256([]byte(unsigned))
	sig, err := rsa.SignPKCS1v15(nil, priv, crypto.SHA256, hash[:])
	if err != nil {
		return "", err
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

type tokResp struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
}

func fetchAccessToken(ctx context.Context, assertion string, hc *http.Client) (string, time.Time, error) {
	form := url.Values{
		"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"},
		"assertion":  {assertion},
	}

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		"https://oauth2.googleapis.com/token", strings.NewReader(form.Encode()))
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

	var tr tokResp
	if err := json.NewDecoder(res.Body).Decode(&tr); err != nil {
		return "", time.Time{}, err
	}
	return tr.AccessToken, time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second), nil
}
