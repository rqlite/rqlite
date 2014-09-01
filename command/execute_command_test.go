package command

import (
	"reflect"
	"testing"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) {
	TestingT(t)
}

type ExecuteCommandSuite struct{}

var _ = Suite(&ExecuteCommandSuite{})

/*
 * ExecuteCommand tests
 *
 * These are somewhat trivial right now.
 */

func (s *ExecuteCommandSuite) Test_NewExecuteCommand(c *C) {
	e := NewExecuteCommand("stmt1")
	c.Assert(e, NotNil)
	c.Assert(e.Stmt, Equals, "stmt1")
	c.Assert(e.CommandName(), Equals, "execute")
}

func (s *ExecuteCommandSuite) Test_NewTransactionExecuteCommandSet(c *C) {
	e := NewTransactionExecuteCommandSet([]string{"stmt1"})
	c.Assert(e, NotNil)
	c.Assert(reflect.DeepEqual(e.Stmts, []string{"stmt1"}), Equals, true)
	c.Assert(e.CommandName(), Equals, "transaction_execute")
}
