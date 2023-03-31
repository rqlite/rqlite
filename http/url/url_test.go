package url

import (
	"net/url"
	"testing"
)

// write a unit test for StringsToURLs
func Test_StringsToURLs(t *testing.T) {
	tests := []struct {
		ss   []string
		urls []*url.URL
	}{
		{
			ss:   nil,
			urls: []*url.URL{},
		},
		{
			ss:   []string{},
			urls: []*url.URL{},
		},
		{
			ss: []string{"http://localhost:4001"},
			urls: []*url.URL{
				{Scheme: "http", Host: "localhost:4001"},
			},
		},
		{
			ss: []string{"http://localhost:4001/"},
			urls: []*url.URL{
				{Scheme: "http", Host: "localhost:4001", Path: "/"},
			},
		},
		{
			ss: []string{"https://localhost:4001"},
			urls: []*url.URL{
				{Scheme: "https", Host: "localhost:4001"},
			},
		},
		{
			ss: []string{"localhost:4001"},
			urls: []*url.URL{
				{Scheme: "http", Host: "localhost:4001"},
			},
		},
		{
			ss: []string{"localhost"},
			urls: []*url.URL{
				{Scheme: "http", Host: "localhost"},
			},
		},
		{
			ss: []string{"localhost", "localhost:4001"},
			urls: []*url.URL{
				{Scheme: "http", Host: "localhost"},
				{Scheme: "http", Host: "localhost:4001"},
			},
		},
		{
			ss: []string{"localhost", "localhost:4001", "https://localhost:4001"},
			urls: []*url.URL{
				{Scheme: "http", Host: "localhost"},
				{Scheme: "http", Host: "localhost:4001"},
				{Scheme: "https", Host: "localhost:4001"},
			},
		},
	}

	for _, tt := range tests {
		urls, err := StringsToURLs(tt.ss)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(urls) != len(tt.urls) {
			t.Fatalf("length mismatch: want %v, got %v", len(tt.urls), len(urls))
		}
		for i, u := range urls {
			if u.Scheme != tt.urls[i].Scheme {
				t.Fatalf("scheme mismatch: want %v, got %v", tt.urls[i].Scheme, u.Scheme)
			}
			if u.Host != tt.urls[i].Host {
				t.Fatalf("host mismatch: want %v, got %v", tt.urls[i].Host, u.Host)
			}
		}
	}
}

func Test_NormalizeAddr(t *testing.T) {
	tests := []struct {
		orig string
		norm string
	}{
		{
			orig: "http://localhost:4001",
			norm: "http://localhost:4001",
		},
		{
			orig: "https://localhost:4001",
			norm: "https://localhost:4001",
		},
		{
			orig: "https://localhost:4001/foo",
			norm: "https://localhost:4001/foo",
		},
		{
			orig: "localhost:4001",
			norm: "http://localhost:4001",
		},
		{
			orig: "localhost",
			norm: "http://localhost",
		},
		{
			orig: ":4001",
			norm: "http://:4001",
		},
	}

	for _, tt := range tests {
		if NormalizeAddr(tt.orig) != tt.norm {
			t.Fatalf("%s not normalized correctly, got: %s", tt.orig, tt.norm)
		}
	}
}

func Test_EnsureHTTPS(t *testing.T) {
	tests := []struct {
		orig    string
		ensured string
	}{
		{
			orig:    "http://localhost:4001",
			ensured: "https://localhost:4001",
		},
		{
			orig:    "https://localhost:4001",
			ensured: "https://localhost:4001",
		},
		{
			orig:    "https://localhost:4001/foo",
			ensured: "https://localhost:4001/foo",
		},
		{
			orig:    "localhost:4001",
			ensured: "https://localhost:4001",
		},
	}

	for _, tt := range tests {
		if e := EnsureHTTPS(tt.orig); e != tt.ensured {
			t.Fatalf("%s not HTTPS ensured correctly, exp %s, got %s", tt.orig, tt.ensured, e)
		}
	}
}

func Test_AddBasicAuth(t *testing.T) {
	var u string
	var err error

	u, err = AddBasicAuth("http://example.com", "user1", "pass1")
	if err != nil {
		t.Fatalf("failed to add user info: %s", err.Error())
	}
	if exp, got := "http://user1:pass1@example.com", u; exp != got {
		t.Fatalf("wrong URL created, exp %s, got %s", exp, got)
	}

	u, err = AddBasicAuth("http://example.com", "user1", "")
	if err != nil {
		t.Fatalf("failed to add user info: %s", err.Error())
	}
	if exp, got := "http://user1:@example.com", u; exp != got {
		t.Fatalf("wrong URL created, exp %s, got %s", exp, got)
	}

	u, err = AddBasicAuth("http://example.com", "", "pass1")
	if err != nil {
		t.Fatalf("failed to add user info: %s", err.Error())
	}
	if exp, got := "http://example.com", u; exp != got {
		t.Fatalf("wrong URL created, exp %s, got %s", exp, got)
	}

	u, err = AddBasicAuth("http://user1:pass1@example.com", "user2", "pass2")
	if err == nil {
		t.Fatalf("failed to get expected error when UserInfo exists")
	}
}

func Test_RemoveBasicAuth(t *testing.T) {
	tests := []struct {
		orig    string
		removed string
	}{
		{
			orig:    "localhost",
			removed: "localhost",
		},
		{
			orig:    "http://localhost:4001",
			removed: "http://localhost:4001",
		},
		{
			orig:    "https://foo:bar@localhost",
			removed: "https://foo:xxxxx@localhost",
		},
		{
			orig:    "https://foo:bar@localhost:4001",
			removed: "https://foo:xxxxx@localhost:4001",
		},
		{
			orig:    "http://foo:bar@localhost:4001/path",
			removed: "http://foo:xxxxx@localhost:4001/path",
		},
	}

	for _, tt := range tests {
		if e := RemoveBasicAuth(tt.orig); e != tt.removed {
			t.Fatalf("%s BasicAuth not removed correctly, exp %s, got %s", tt.orig, tt.removed, e)
		}
	}
}
