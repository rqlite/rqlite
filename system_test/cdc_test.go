package system

import (
	"fmt"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/cdc"
	"github.com/rqlite/rqlite/v8/cdc/cdctest"
)

func Test_CDC_SingleNode(t *testing.T) {
	node := mustNewLeaderNode("node1")
	defer node.Deprovision()

	// Configure CDC before opening the store.
	testEndpoint := cdctest.NewHTTPTestServer()
	cdcCfg := cdc.DefaultConfig()
	cdcCfg.Endpoint = testEndpoint.URL

	cdcCluster := cdc.NewCDCCluster(node.Store, node.Cluster, node.Client)
	cdcService, err := cdc.NewService(node.ID, node.Dir, cdcCluster, cdcCfg)
	if err != nil {
		panic(fmt.Sprintf("failed to create CDC service: %s", err.Error()))
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

func Test_CDC_MultiNode(t *testing.T) {
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
	for _, node := range []*Node{node1, node2, node3} {
		cdcCfg := cdc.DefaultConfig()
		cdcCfg.HighWatermarkInterval = 100 * time.Millisecond
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
	}, 100*time.Millisecond, 5*time.Second)

	testEndpoint.ClearRequests()

	// Kill the leader, ensure future changes are still captured.
	node1.Deprovision()
	cluster := Cluster{node2, node3}
	cluster.WaitForNewLeader(node1)

	_, err = node2.Execute(`INSERT INTO foo (id, name) VALUES (2, 'Bob')`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}
	testPoll(t, func() (bool, error) {
		return testEndpoint.GetMessageCount() == 1, nil
	}, 100*time.Millisecond, 2*time.Second)
}
