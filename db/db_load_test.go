package db

import (
	"testing"
)

func Test_TableCreationInMemoryLoad(t *testing.T) {
	db := mustCreateInMemoryDatabase()
	defer db.Close()

	if !db.InMemory() {
		t.Fatal("in-memory database marked as not in-memory")
	}

	r, err := db.ExecuteStringStmt("CREATE TABLE logs (entry TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for table create, expected %s, got %s", exp, got)
	}

	go func() {
		for {
			r, err = db.ExecuteStringStmt(`INSERT INTO logs(entry) VALUES("hello")`)
			if err != nil {
				return
			}
		}
	}()

	n := 1
	for {
		res, err := db.QueryStringStmt("SELECT COUNT(*) FROM logs")
		if err != nil {
			t.Fatalf("failed to query table: %s", err.Error())
		}
		if exp, got := 1, len(res); exp != got {
			t.Fatalf("wrong number of rows returned, exp %d, got %d", exp, got)
		}
		if res[0].Error != "" {
			t.Fatalf("query rows has an error after %d queries: %s", n, res[0].Error)
		}
		n++
	}
}
