package consul

import (
	"math/rand"
	"strings"
	"testing"
)

func Test_NewClient(t *testing.T) {
	c, err := New("rqlite")
	if err != nil {
		t.Fatalf("failed to create new client: %s", err.Error())
	}
	if c == nil {
		t.Fatalf("returned client is nil")
	}
}

func Test_InitializeLeader(t *testing.T) {
	c, _ := New(randomString())
	_, _, _, ok, err := c.GetLeader()
	if err != nil {
		t.Fatalf("failed to GetLeader: %s", err.Error())
	}
	if ok {
		t.Fatalf("leader found when not expected")
	}

	ok, err = c.InitializeLeader("1", "http://localhost:4001", "localhost:4002")
	if err != nil {
		t.Fatalf("error when initializing leader: %s", err.Error())
	}
	if !ok {
		t.Fatalf("failed to initialize leader")
	}

	id, api, addr, ok, err := c.GetLeader()
	if err != nil {
		t.Fatalf("failed to GetLeader: %s", err.Error())
	}
	if !ok {
		t.Fatalf("leader not found when expected")
	}
	if id != "1" || api != "http://localhost:4001" || addr != "localhost:4002" {
		t.Fatalf("retrieved incorrect details for leader")
	}
}

func Test_InitializeLeaderConflict(t *testing.T) {
	c, _ := New(randomString())
	_, _, _, ok, err := c.GetLeader()
	if err != nil {
		t.Fatalf("failed to GetLeader: %s", err.Error())
	}
	if ok {
		t.Fatalf("leader found when not expected")
	}

	err = c.SetLeader("2", "http://localhost:4003", "localhost:4004")
	if err != nil {
		t.Fatalf("error when setting leader: %s", err.Error())
	}

	ok, err = c.InitializeLeader("1", "http://localhost:4001", "localhost:4002")
	if err != nil {
		t.Fatalf("error when initializing leader: %s", err.Error())
	}
	if ok {
		t.Fatalf("initialized leader when should have failed")
	}

	id, api, addr, ok, err := c.GetLeader()
	if err != nil {
		t.Fatalf("failed to GetLeader: %s", err.Error())
	}
	if !ok {
		t.Fatalf("leader not found when expected")
	}
	if id != "2" || api != "http://localhost:4003" || addr != "localhost:4004" {
		t.Fatalf("retrieved incorrect details for leader")
	}
}

func randomString() string {
	var output strings.Builder
	chars := "abcdedfghijklmnopqrstABCDEFGHIJKLMNOP"
	for i := 0; i < 10; i++ {
		random := rand.Intn(len(chars))
		randomChar := chars[random]
		output.WriteString(string(randomChar))
	}
	return output.String()
}
