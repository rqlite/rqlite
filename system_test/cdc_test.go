package system

import (
	"fmt"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/cdc"
	"github.com/rqlite/rqlite/v8/cdc/cdctest"
)

func Test_CDC_SingleNode(t *testing.T) {
	testFn := func(t *testing.T, failRate int) {
		node := mustNewLeaderNode("node1")
		defer node.Deprovision()

		// Configure CDC before opening the store.
		testEndpoint := cdctest.NewHTTPTestServer()
		testEndpoint.SetFailRate(failRate)
		cdcCfg := cdc.DefaultConfig()
		cdcCfg.TransmitMaxRetries = 100 // Keep retrying for a while.
		cdcCfg.TransmitMinBackoff = 50 * time.Millisecond
		cdcCfg.TransmitMaxBackoff = 50 * time.Millisecond
		cdcCfg.Endpoint = testEndpoint.URL

		cdcCluster := cdc.NewCDCCluster(node.Store, node.Cluster, node.Client)
		cdcService, err := cdc.NewService(node.ID, node.Dir, cdcCluster, cdcCfg)
		if err != nil {
			t.Fatalf("failed to create CDC service: %s", err.Error())
		}
		node.CDC = cdcService
		node.CDC.Start()
		node.CDC.SetLeader(true)

		node.Store.EnableCDC(node.CDC.C(), false)

		_, err = node.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`)
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
		}, 100*time.Millisecond, 5*time.Second)
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

func Test_CDC_MultiNode(t *testing.T) {

	testFn := func(t *testing.T, failRate int) {
		node1 := mustNewLeaderNode("node1")
		defer node1.Deprovision()
		node2 := mustNewNode("node2", false)
		defer node2.Deprovision()
		if err := node2.Join(node1); err != nil {
			t.Fatalf("node failed to join leader: %s", err.Error())
		}
		_, err := node2.WaitForLeader()
		if err != nil {
			t.Fatalf("failed waiting for leader: %s", err.Error())
		}
		node3 := mustNewNode("node3", false)
		defer node3.Deprovision()
		if err := node3.Join(node1); err != nil {
			t.Fatalf("node failed to join leader: %s", err.Error())
		}
		_, err = node3.WaitForLeader()
		if err != nil {
			t.Fatalf("failed waiting for leader: %s", err.Error())
		}

		// Configure CDC service for each node.
		testEndpoint := cdctest.NewHTTPTestServer()
		testEndpoint.DumpRequest = false
		testEndpoint.SetFailRate(failRate)
		for _, node := range []*Node{node1, node2, node3} {
			cdcCfg := cdc.DefaultConfig()
			cdcCfg.HighWatermarkInterval = 100 * time.Millisecond
			cdcCfg.TransmitMaxRetries = 100 // Keep retrying for a while.
			cdcCfg.TransmitMinBackoff = 50 * time.Millisecond
			cdcCfg.TransmitMaxBackoff = 50 * time.Millisecond
			cdcCfg.Endpoint = testEndpoint.URL
			cdcCluster := cdc.NewCDCCluster(node.Store, node.Cluster, node.Client)
			cdcService, err := cdc.NewService(node.ID, node.Dir, cdcCluster, cdcCfg)
			if err != nil {
				panic(fmt.Sprintf("failed to create CDC service: %s", err.Error()))
			}
			node.CDC = cdcService
			node.CDC.Start()
			node.Store.EnableCDC(node.CDC.C(), false)
		}

		node1.CDC.SetLeader(true)

		_, err = node1.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}
		_, err = node1.Execute(`INSERT INTO foo (id, name) VALUES (1, 'Alice')`)
		if err != nil {
			t.Fatalf("failed to insert data: %v", err)
		}
		_, err = node1.Execute(`UPDATE foo SET name = 'Alice Updated' WHERE id = 1`)
		if err != nil {
			t.Fatalf("failed to update data: %v", err)
		}
		_, err = node1.Execute(`DELETE FROM foo WHERE id = 1`)
		if err != nil {
			t.Fatalf("failed to delete data: %v", err)
		}

		testPoll(t, func() (bool, error) {
			// 1 create, 1 insert, 1 update, 1 delete
			return testEndpoint.GetMessageCount() == 4, nil
		}, 100*time.Millisecond, 10*time.Second)

		hi := testEndpoint.GetHighestMessageIndex()
		testPoll(t, func() (bool, error) {
			return node1.CDC.HighWatermark() == hi, nil
		}, 100*time.Millisecond, 10*time.Second)

		// Wait the highwater mark to be replicated to other nodes.
		testPoll(t, func() (bool, error) {
			f := node2.CDC.HighWatermark() == hi && node3.CDC.HighWatermark() == hi
			return f, nil
		}, 100*time.Millisecond, 10*time.Second)

		testEndpoint.Reset()
		if testEndpoint.GetMessageCount() != 0 {
			t.Fatalf("expected 0 messages after clear, got %d", testEndpoint.GetMessageCount())
		}

		// Verify that endpoint fails and service retries line up.
		if exp, got := testEndpoint.GetFailedRequestCount(), int64(node1.CDC.NumEndpointRetries()); exp != got {
			t.Fatalf("expected %d endpoint retries, got %d", exp, got)
		}

		// Kill the leader, ensure future changes are still sent to the endpoint.
		node1.Deprovision()
		cluster := Cluster{node2, node3}
		newLeader, err := cluster.WaitForNewLeader(node1)
		if err != nil {
			t.Fatalf("failed waiting for new leader: %s", err.Error())
		}

		_, err = node2.Execute(`INSERT INTO foo (id, name) VALUES (2, 'Bob')`)
		if err != nil {
			t.Fatalf("failed to insert data: %v", err)
		}
		testPoll(t, func() (bool, error) {
			return testEndpoint.GetMessageCount() == 1, nil
		}, 100*time.Millisecond, 10*time.Second)
		if testEndpoint.GetRequestCount() != 1 {
			t.Fatalf("expected 1 request, got %d", testEndpoint.GetRequestCount())
		}

		// Join another node, check that it picks up the highwater mark.
		node4 := mustNewNode("node4", false)
		defer node4.Deprovision()
		if err := node4.Join(newLeader); err != nil {
			t.Fatalf("node failed to join leader: %s", err.Error())
		}
		_, err = node4.WaitForLeader()
		if err != nil {
			t.Fatalf("failed waiting for leader: %s", err.Error())
		}
		cdcCfg := cdc.DefaultConfig()
		cdcCfg.HighWatermarkInterval = 100 * time.Millisecond
		cdcCfg.Endpoint = testEndpoint.URL
		cdcCluster := cdc.NewCDCCluster(node4.Store, node4.Cluster, node4.Client)
		cdcService, err := cdc.NewService(node4.ID, node4.Dir, cdcCluster, cdcCfg)
		if err != nil {
			t.Fatalf("failed to create CDC service: %s", err.Error())
		}
		node4.CDC = cdcService
		node4.CDC.Start()
		node4.Store.EnableCDC(node4.CDC.C(), false)
		testPoll(t, func() (bool, error) {
			return node4.CDC.HighWatermark() == testEndpoint.GetHighestMessageIndex(), nil
		}, 100*time.Millisecond, 10*time.Second)
	}

	t.Run("NoFail", func(t *testing.T) {
		testFn(t, 0)
	})
	t.Run("Fail_10Percent", func(t *testing.T) {
		testFn(t, 10)
	})
	t.Run("Fail_50Percent", func(t *testing.T) {
		testFn(t, 50)
	})
	t.Run("Fail_90Percent", func(t *testing.T) {
		testFn(t, 90)
	})
}
