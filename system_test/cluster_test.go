package system

import (
	"testing"
)

func Test_JoinLeaderNode(t *testing.T) {
	leader := mustNewLeaderNode()
	defer leader.Deprovision()

	node := mustNewNode(false)
	defer node.Deprovision()
	if err := node.Join(leader); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	node.WaitForLeader()
}
