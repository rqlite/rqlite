package db

import (
	"fmt"
	"testing"
)

func Test_TableCreationInMemoryFTSLoad(t *testing.T) {
	db := mustCreateInMemoryDatabase()
	defer db.Close()

	if !db.InMemory() {
		t.Fatal("in-memory database marked as not in-memory")
	}

	r, err := db.ExecuteStringStmt("CREATE VIRTUAL TABLE logs USING fts4(entry)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for table create, expected %s, got %s", exp, got)
	}

	go func() {
		for {
			r, err = db.ExecuteStringStmt(`INSERT INTO logs(entry) VALUES("13.66.139.0 - - [19/Dec/2020:13:57:26 +0100] GET /index.php?option=com_phocagallery&view=category&id=1:almhuette-raith&Itemid=53 HTTP/1.1 200 32653 - Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm) -")`)
			if err != nil {
				return
			}
		}
	}()

	for {
		res, err := db.QueryStringStmt("SELECT COUNT(*) FROM logs")
		if err != nil {
			t.Fatalf("failed to query empty table: %s", err.Error())
		}
		fmt.Println(res)
	}
}
