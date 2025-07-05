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

type Config struct {
	UploadEndpoint string // defaults to https://storage.googleapis.com
	Bucket         string
	ProjectID      string
	ObjectName     string
	CredentialPath string
}

type GCSClient struct {
	cfg Config

	sa          serviceAccount
	accessToken string
	expiry      time.Time
	tokenMu     sync.Mutex

	http      *http.Client
	uploadURL string
	objectURL string
	bucketURL string
}

func New(cfg Config) (*GCSClient, error) {
	if cfg.UploadEndpoint == "" {
		cfg.UploadEndpoint = "https://storage.googleapis.com"
	}
	sa, err := loadServiceAccount(cfg.CredentialPath)
	if err != nil {
		return nil, err
	}
	base := strings.TrimRight(cfg.UploadEndpoint, "/")

	return &GCSClient{
		cfg:       cfg,
		sa:        *sa,
		http:      &http.Client{},
		uploadURL: fmt.Sprintf("%s/upload/storage/v1/b/%s/o", base, url.PathEscape(cfg.Bucket)),
		objectURL: fmt.Sprintf("%s/storage/v1/b/%s/o/%s",
			base, url.PathEscape(cfg.Bucket), url.PathEscape(cfg.ObjectName)),
		bucketURL: fmt.Sprintf("%s/storage/v1/b/%s", base, url.PathEscape(cfg.Bucket)),
	}, nil
}

func (c *GCSClient) EnsureBucket(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.bucketURL, nil)
	if err != nil {
		return err
	}
	if err := c.addAuth(req); err != nil {
		return err
	}
	res, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	switch res.StatusCode {
	case http.StatusOK:
		return nil // already exists
	case http.StatusNotFound:
		body := fmt.Sprintf(`{"name":"%s"}`, c.cfg.Bucket)
		u := c.bucketURL + "?project=" + url.QueryEscape(c.cfg.ProjectID)
		req, _ = http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if err := c.addAuth(req); err != nil {
			return err
		}
		res, err = c.http.Do(req)
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

func (c *GCSClient) Upload(ctx context.Context, r io.Reader, id string) error {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)

	meta := fmt.Sprintf(`{"name":"%s","metadata":{"id":"%s"}}`, c.cfg.ObjectName, id)
	hdr := textproto.MIMEHeader{"Content-Type": {"application/json"}}
	part, _ := w.CreatePart(hdr)
	part.Write([]byte(meta))

	hdr = textproto.MIMEHeader{"Content-Type": {"application/octet-stream"}}
	part, _ = w.CreatePart(hdr)
	if _, err := io.Copy(part, r); err != nil {
		return err
	}
	w.Close()

	u := c.uploadURL + "?uploadType=multipart"
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, u, &buf)
	req.Header.Set("Content-Type", "multipart/related; boundary="+w.Boundary())
	if err := c.addAuth(req); err != nil {
		return err
	}

	res, err := c.http.Do(req)
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

func (c *GCSClient) Download(ctx context.Context, w io.WriterAt) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.objectURL+"?alt=media", nil)
	if err := c.addAuth(req); err != nil {
		return err
	}
	res, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("download failed: %s", b)
	}

	buf := make([]byte, 32*1024)
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

func (c *GCSClient) Delete(ctx context.Context) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodDelete, c.objectURL, nil)
	if err := c.addAuth(req); err != nil {
		return err
	}
	res, err := c.http.Do(req)
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

func (c *GCSClient) CurrentID(ctx context.Context) (string, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.objectURL, nil)
	if err := c.addAuth(req); err != nil {
		return "", err
	}
	res, err := c.http.Do(req)
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
	tok, err := c.getToken(req.Context())
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	return nil
}

func (c *GCSClient) getToken(ctx context.Context) (string, error) {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()

	if time.Until(c.expiry) > 2*time.Minute {
		return c.accessToken, nil
	}

	jwt, err := makeJWT(&c.sa)
	if err != nil {
		return "", err
	}
	tok, exp, err := fetchToken(ctx, jwt, c.http)
	if err != nil {
		return "", err
	}
	c.accessToken, c.expiry = tok, exp
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
		Scope: "https://www.googleapis.com/auth/devstorage.read_write",
		Aud:   "https://oauth2.googleapis.com/token",
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
		"https://oauth2.googleapis.com/token", strings.NewReader(data.Encode()))
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
