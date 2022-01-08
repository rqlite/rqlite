package http

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestClient_QueryWhenAllAvailable(t *testing.T) {
	node1 := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
	}))
	defer node1.Close()

	node2 := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
	}))
	defer node2.Close()

	httpClient := http.DefaultClient

	u1, _ := url.Parse(node1.URL)
	u2, _ := url.Parse(node2.URL)
	client := NewClient(httpClient, []string{u1.Host, u2.Host})
	res, err := client.Query(url.URL{
		Path: "/",
	})
	defer res.Body.Close()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if client.currentHost != 0 {
		t.Errorf("expected to only forward requests to the first host")
	}
}

func TestClient_QueryWhenSomeAreAvailable(t *testing.T) {
	node1 := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer node1.Close()

	node2 := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
	}))
	defer node2.Close()

	httpClient := http.DefaultClient

	u1, _ := url.Parse(node1.URL)
	u2, _ := url.Parse(node2.URL)
	client := NewClient(httpClient, []string{u1.Host, u2.Host})
	res, err := client.Query(url.URL{
		Path: "/",
	})
	defer res.Body.Close()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if client.currentHost != 1 {
		t.Errorf("expected to move on to the following host")
	}
}

func TestClient_QueryWhenAllUnavailable(t *testing.T) {
	node1 := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer node1.Close()

	node2 := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer node2.Close()

	httpClient := http.DefaultClient

	u1, _ := url.Parse(node1.URL)
	u2, _ := url.Parse(node2.URL)
	client := NewClient(httpClient, []string{u1.Host, u2.Host})
	_, err := client.Query(url.URL{
		Path: "/",
	})

	if err == nil {
		t.Errorf("expected error")
	}

	if err != ErrNoAvailableHost {
		t.Errorf("Expected %v, got: %v", ErrNoAvailableHost, err)
	}
}

func TestClient_BasicAuthIsForwarded(t *testing.T) {
	mockAuth := func(request *http.Request) bool {
		user, pass, ok := request.BasicAuth()
		if ok {
			if user == "john" && pass == "doe" {
				return true
			}
		}
		return false
	}
	node1 := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if mockAuth(request) {
			writer.WriteHeader(http.StatusOK)
			return
		}
		writer.WriteHeader(http.StatusUnauthorized)
	}))
	defer node1.Close()

	node2 := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if mockAuth(request) {
			writer.WriteHeader(http.StatusOK)
			return
		}
		writer.WriteHeader(http.StatusUnauthorized)
	}))
	defer node2.Close()

	httpClient := http.DefaultClient

	u1, _ := url.Parse(node1.URL)
	u2, _ := url.Parse(node2.URL)
	client := NewClient(httpClient, []string{u1.Host, u2.Host}, WithBasicAuth("john:wrongpassword"))

	res, err := client.Query(url.URL{
		Path: "/",
	})

	if err != nil {
		t.Errorf("unexpected error")
	}

	if res.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected unauthorized status")
	}
}
