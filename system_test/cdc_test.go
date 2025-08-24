package system

import (
	"testing"
	"time"
)

func Test_CDC_SingleNode(t *testing.T) {
	node := mustNewLeaderNode("node1")
	defer node.Deprovision()

	node.Store.EnableCDC(node.CDC.C(), false)

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
		return node.CDCEndpoint.GetMessageCount() == 4, nil
	}, 100*time.Millisecond, 5*time.Second)
}
