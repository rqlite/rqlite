package system

import (
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/cdc"
	"github.com/rqlite/rqlite/v8/command/proto"
)

func Test_CDC_SingleNode(t *testing.T) {
	node := mustNewLeaderNode("node1")
	defer node.Deprovision()

	ch := make(chan *proto.CDCIndexedEventGroup, 5)
	cdcCfg := cdc.DefaultConfig()
	cdcCfg.LogOnly = true
	cdcCluster := cdc.NewCDCCluster(node.Store, node.Cluster, node.Client)
	cdcService, err := cdc.NewService(node.ID, node.Dir, cdcCluster, ch, cdcCfg)
	if err != nil {
		t.Fatalf("failed to create CDC service: %v", err)
	}
	if err := cdcService.Start(); err != nil {
		t.Fatalf("failed to start CDC service: %v", err)
	}
	if err := node.Store.EnableCDC(ch, false); err != nil {
		t.Fatalf("failed to enable CDC: %v", err)
	}

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

	time.Sleep(5 * time.Second) // Allow time for CDC to process events
}
