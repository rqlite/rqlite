package system

import (
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/cdc"
	"github.com/rqlite/rqlite/v8/cdc/cdctest"
)

func Test_CDC_SingleNode(t *testing.T) {
	testFn := func(t *testing.T, failRate int) {
		testEndpoint := cdctest.NewHTTPTestServer()
		testEndpoint.SetFailRate(failRate)
		testEndpoint.Start()
		defer testEndpoint.Close()

		// Create node with CDC support using helper
		node := mustNewLeaderNodeWithCDC("node1", testEndpoint.URL())
		defer node.Deprovision()

		_, err := node.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}
		_, err = node.Execute(`INSERT INTO foo (id, name) VALUES (1, 'Alice')`)
		if err != nil {
			t.Fatalf("failed to insert data: %v", err)
		}
		_, err = node.Execute(`UPDATE foo SET name = 'Alice Updated' WHERE id = 1`)
		if err != nil {
			t.Fatalf("failed to update data: %v", err)
		}
		_, err = node.Execute(`DELETE FROM foo WHERE id = 1`)
		if err != nil {
			t.Fatalf("failed to delete data: %v", err)
		}

		testPoll(t, func() (bool, error) {
			// 1 create, 1 insert, 1 update, 1 delete
			return testEndpoint.GetMessageCount() == 4, nil
		}, 100*time.Millisecond, 10*time.Second)
	}

	t.Run("NoFail", func(t *testing.T) {
		testFn(t, 0)
	})
	t.Run("Fail_10Percent", func(t *testing.T) {
		testFn(t, 10)
	})
	t.Run("Fail_90Percent", func(t *testing.T) {
		testFn(t, 90)
	})
}

// Test_CDC_SingleNode_LaterStart verifies that starting the CDC service
// before the HTTP endpoint is available works as expected.
func Test_CDC_SingleNode_LaterStart(t *testing.T) {
	testEndpoint := cdctest.NewHTTPTestServer()

	// Create node with CDC support using helper
	node := mustNewLeaderNodeWithCDC("node1", testEndpoint.URL())
	defer node.Deprovision()

	_, err := node.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	_, err = node.Execute(`INSERT INTO foo (id, name) VALUES (1, 'Alice')`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}
	_, err = node.Execute(`UPDATE foo SET name = 'Alice Updated' WHERE id = 1`)
	if err != nil {
		t.Fatalf("failed to update data: %v", err)
	}
	_, err = node.Execute(`DELETE FROM foo WHERE id = 1`)
	if err != nil {
		t.Fatalf("failed to delete data: %v", err)
	}

	testEndpoint.Start()
	defer testEndpoint.Close()

	testPoll(t, func() (bool, error) {
		// 1 create, 1 insert, 1 update, 1 delete
		return testEndpoint.GetMessageCount() == 4, nil
	}, 100*time.Millisecond, 5*time.Second)
}

func Test_CDC_MultiNode(t *testing.T) {
	// TODO: Multi-node CDC tests need to be redesigned to create nodes with CDC configuration
	// from the start, rather than attempting to configure CDC on already-opened stores.
	// This is because SetCDCConfig has been removed and all CDC configuration must be done
	// via store.New() during store creation.
	t.Skip("Multi-node CDC tests need redesign after SetCDCConfig removal")
}

func mustCDCConfig(url string) *cdc.Config {
	cdcCfg := cdc.DefaultConfig()
	cdcCfg.HighWatermarkInterval = 100 * time.Millisecond
	cdcCfg.TransmitMaxRetries = 100 // Keep retrying for a while.
	cdcCfg.TransmitMinBackoff = 50 * time.Millisecond
	cdcCfg.TransmitMaxBackoff = 50 * time.Millisecond
	cdcCfg.Endpoint = url
	return cdcCfg
}
