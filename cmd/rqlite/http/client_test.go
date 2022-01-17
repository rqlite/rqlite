package http

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestClient_InvalidHost(t *testing.T) {
	if _, err := NewClient(http.DefaultClient, []string{"127.0.0.1:4001"}); err == nil {
		t.Fatalf(`expected "%s" when creating client with invalid host`, ErrProtocolNotSpecified)
	}
	if _, err := NewClient(http.DefaultClient, []string{"http://127.0.0.1", "127.0.0.1:4001"}); err == nil {
		t.Fatalf(`expected "%s" when creating client with invalid host`, ErrProtocolNotSpecified)
	}
}

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

	client, err := NewClient(httpClient, []string{node1.URL, node2.URL})
	if err != nil {
		t.Fatalf("failed to create client: %s", err.Error())
	}
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

	if res.StatusCode != http.StatusOK {
		t.Errorf("unexpected status code, expected '200' got '%d'", res.StatusCode)
	}
}

func TestClient_QueryWhenSomeAreAvailable(t *testing.T) {
	node1 := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
	}))

	node2 := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
	}))
	defer node2.Close()

	httpClient := http.DefaultClient

	// Shutting down one of the hosts making it unavailable
	node1.Close()

	client, err := NewClient(httpClient, []string{node1.URL, node2.URL})
	if err != nil {
		t.Fatalf("failed to create client: %s", err.Error())
	}
	res, err := client.Query(url.URL{
		Path: "/",
	})
	defer res.Body.Close()

	// If the request succeeds after changing hosts, it should be reflected in the returned error
	// as HostChangedError
	if err == nil {
		t.Errorf("expected HostChangedError got nil instead")
	}

	hcer, ok := err.(*HostChangedError)

	if !ok {
		t.Errorf("unexpected error occurred: %v", err)
	}

	if hcer.NewHost != node2.URL {
		t.Errorf("unexpected responding host")
	}

	if client.currentHost != 1 {
		t.Errorf("expected to move on to the following host")
	}

	if res.StatusCode != http.StatusOK {
		t.Errorf("unexpected status code, expected '200' got '%d'", res.StatusCode)
	}
}

func TestClient_QueryWhenAllUnavailable(t *testing.T) {
	node1 := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
	}))

	node2 := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
	}))

	httpClient := http.DefaultClient

	// Shutting down both nodes, both of them now are unavailable
	node1.Close()
	node2.Close()
	client, err := NewClient(httpClient, []string{node1.URL, node2.URL})
	if err != nil {
		t.Fatalf("failed to create client: %s", err.Error())
	}
	_, err = client.Query(url.URL{
		Path: "/",
	})

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

	client, err := NewClient(httpClient, []string{node1.URL, node2.URL}, WithBasicAuth("john:wrongpassword"))
	if err != nil {
		t.Fatalf("failed to create client: %s", err.Error())
	}

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
