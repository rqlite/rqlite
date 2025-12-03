package system

import (
"testing"
)

func Test_SingleNode_RETURNING_DescColumn(t *testing.T) {
node := mustNewLeaderNode("leader1")
defer node.Deprovision()

// Create table with desc column
res, err := node.Execute(`CREATE TABLE t (desc TEXT)`)
if err != nil {
t.Fatalf(`CREATE TABLE failed: %s`, err.Error())
}
if got, exp := res, `{"results":[{}]}`; got != exp {
t.Fatalf("wrong CREATE TABLE result, exp %s, got %s", exp, got)
}

// Insert data
res, err = node.Execute(`INSERT INTO t VALUES('')`)
if err != nil {
t.Fatalf(`INSERT failed: %s`, err.Error())
}
if got, exp := res, `{"results":[{"last_insert_id":1,"rows_affected":1}]}`; got != exp {
t.Fatalf("wrong INSERT result, exp %s, got %s", exp, got)
}

// Try UPDATE with RETURNING on desc column
res, err = node.Execute(`UPDATE t SET desc = 'd1' WHERE desc = '' RETURNING *`)
if err != nil {
t.Fatalf(`UPDATE with RETURNING failed: %s`, err.Error())
}
t.Logf("UPDATE with RETURNING result: %s", res)
// This should return columns and values, not just rows_affected
if got, exp := res, `{"results":[{"columns":["desc"],"types":["text"],"values":[["d1"]]}]}`; got != exp {
t.Fatalf("wrong UPDATE with RETURNING result, exp %s, got %s", exp, got)
}
}
