package json

import (
	"fmt"
	"testing"

	"github.com/rqlite/rqlite/v8/command/proto"
)

func Test_Single(t *testing.T) {
	e := &proto.CDCIndexedEventGroup{
		Index:           1,
		CommitTimestamp: 123456789,
		Events: []*proto.CDCEvent{
			{
				Op:       proto.CDCEvent_INSERT,
				Table:    "users",
				NewRowId: 42,
			},
		},
	}

	b, err := WriteJSON(e)
	if err != nil {
		t.Fatalf("failed to marshal: %s", err)
	}
	fmt.Println(string(b))
}
