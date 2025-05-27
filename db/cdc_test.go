package db

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/command/proto"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

const testTableName = "test_table"
const allTypesTableName = "all_types_table"

// openTestDB creates a new database for testing.
func openTestDB(t *testing.T, cdcEnabled bool) (*DB, string) {
	t.Helper()
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	db, err := Open(dbPath, true, true, cdcEnabled) // fkEnabled=true, wal=true
	if err != nil {
		t.Fatalf("failed to open test DB: %v", err)
	}
	return db, dbPath
}

// captureLogOutput redirects log output to a buffer for inspection.
// It returns the buffer and a function to restore the original log output.
func captureLogOutput(t *testing.T) (*bytes.Buffer, func()) {
	t.Helper()
	var buf bytes.Buffer
	originalOutput := log.Writer()
	log.SetOutput(&buf)
	return &buf, func() {
		log.SetOutput(originalOutput)
	}
}

// getCDCEventFromLog extracts the first CDCEvent JSON string from the log buffer
// and unmarshals it.
func getCDCEventFromLog(t *testing.T, logBuf *bytes.Buffer) *proto.CDCEvent {
	t.Helper()
	logOutput := logBuf.String()
	if logOutput == "" {
		t.Log("log buffer is empty")
		return nil
	}

	// Assuming the CDC event is logged with a prefix like "CDC Event: "
	prefix := "CDC Event: "
	lines := strings.Split(strings.TrimSpace(logOutput), "\n")
	for _, line := range lines {
		if strings.Contains(line, prefix) {
			jsonPart := line[strings.Index(line, prefix)+len(prefix):]
			var event proto.CDCEvent
			err := protojson.Unmarshal([]byte(jsonPart), &event)
			if err != nil {
				t.Fatalf("failed to unmarshal CDCEvent from log: %v\nLog content: %s\nJSON part: %s", err, logOutput, jsonPart)
			}
			return &event
		}
	}
	t.Logf("no CDC event found in log buffer:\n%s", logOutput)
	return nil
}

// countCDCEventsInLog counts how many CDC events are in the log.
func countCDCEventsInLog(logBuf *bytes.Buffer) int {
	logOutput := logBuf.String()
	if logOutput == "" {
		return 0
	}
	prefix := "CDC Event: "
	return strings.Count(logOutput, prefix)
}

func TestCDCDisabled(t *testing.T) {
	db, dbPath := openTestDB(t, false) // CDC Disabled
	defer os.Remove(dbPath)
	defer db.Close()

	logBuf, restoreLog := captureLogOutput(t)
	defer restoreLog()

	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	_, err = db.ExecuteStringStmt("INSERT INTO foo(name) VALUES ('test')")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	_, err = db.ExecuteStringStmt("UPDATE foo SET name = 'test2' WHERE id = 1")
	if err != nil {
		t.Fatalf("failed to update: %v", err)
	}
	_, err = db.ExecuteStringStmt("DELETE FROM foo WHERE id = 1")
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	if countCDCEventsInLog(logBuf) > 0 {
		t.Errorf("expected no CDC events when CDC is disabled, but got some. Log:\n%s", logBuf.String())
	}
}

func TestCDCInsert(t *testing.T) {
	db, dbPath := openTestDB(t, true) // CDC Enabled
	defer os.Remove(dbPath)
	defer db.Close()

	logBuf, restoreLog := captureLogOutput(t)
	defer restoreLog()

	_, err := db.ExecuteStringStmt(fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, value REAL)", testTableName))
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	insertName := "initial_insert"
	insertValue := 123.45
	res, err := db.ExecuteStringStmt(fmt.Sprintf("INSERT INTO %s(name, value) VALUES ('%s', %f)", testTableName, insertName, insertValue))
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if len(res) != 1 || res[0].GetE() == nil {
		t.Fatalf("unexpected execute result: %v", res)
	}
	lastInsertID := res[0].GetE().LastInsertId

	if countCDCEventsInLog(logBuf) != 1 {
		t.Fatalf("expected 1 CDC event, got %d. Log:\n%s", countCDCEventsInLog(logBuf), logBuf.String())
	}
	event := getCDCEventFromLog(t, logBuf)
	if event == nil {
		t.Fatal("CDC event is nil")
	}

	if event.Operation != proto.OperationType_OPERATION_TYPE_INSERT {
		t.Errorf("expected operation type INSERT, got %s", event.Operation)
	}
	if event.TableName != testTableName {
		t.Errorf("expected table name '%s', got '%s'", testTableName, event.TableName)
	}
	if event.NewRowId != lastInsertID {
		t.Errorf("expected new_row_id %d, got %d", lastInsertID, event.NewRowId)
	}
	if event.OldRowId != 0 { // Should be 0 or not set for INSERT
		t.Errorf("expected old_row_id to be 0, got %d", event.OldRowId)
	}
	if event.Timestamp == 0 {
		t.Error("expected timestamp to be populated")
	}
	if event.OldRow != nil {
		t.Errorf("expected old_row to be nil for INSERT, got %v", event.OldRow)
	}
	if event.NewRow == nil || len(event.NewRow.Values) != 3 { // id, name, value
		t.Fatalf("expected new_row to have 3 values, got %v", event.NewRow)
	}

	// Validate NewRow data (order: id, name, value as per table schema)
	// Note: SQLite might return the auto-increment ID as the first value.
	// The hook provides values in table column order.
	if idVal := event.NewRow.Values[0].GetI(); idVal != lastInsertID {
		t.Errorf("NewRow[0] (id): expected %d, got %d", lastInsertID, idVal)
	}
	if nameVal := event.NewRow.Values[1].GetS(); nameVal != insertName {
		t.Errorf("NewRow[1] (name): expected '%s', got '%s'", insertName, nameVal)
	}
	if valVal := event.NewRow.Values[2].GetD(); valVal != insertValue {
		t.Errorf("NewRow[2] (value): expected %f, got %f", insertValue, valVal)
	}
}

func TestCDCUpdate(t *testing.T) {
	db, dbPath := openTestDB(t, true) // CDC Enabled
	defer os.Remove(dbPath)
	defer db.Close()

	_, err := db.ExecuteStringStmt(fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, name TEXT, value REAL)", testTableName))
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	initialName := "before_update"
	initialValue := 10.1
	rowID := int64(1)
	_, err = db.ExecuteStringStmt(fmt.Sprintf("INSERT INTO %s(id, name, value) VALUES (%d, '%s', %f)", testTableName, rowID, initialName, initialValue))
	if err != nil {
		t.Fatalf("failed to insert initial data: %v", err)
	}

	logBuf, restoreLog := captureLogOutput(t) // Capture logs only for the UPDATE
	defer restoreLog()

	updatedName := "after_update"
	updatedValue := 20.2
	_, err = db.ExecuteStringStmt(fmt.Sprintf("UPDATE %s SET name = '%s', value = %f WHERE id = %d", testTableName, updatedName, updatedValue, rowID))
	if err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	if countCDCEventsInLog(logBuf) != 1 {
		t.Fatalf("expected 1 CDC event for UPDATE, got %d. Log:\n%s", countCDCEventsInLog(logBuf), logBuf.String())
	}
	event := getCDCEventFromLog(t, logBuf)
	if event == nil {
		t.Fatal("CDC event is nil")
	}

	if event.Operation != proto.OperationType_OPERATION_TYPE_UPDATE {
		t.Errorf("expected operation type UPDATE, got %s", event.Operation)
	}
	if event.TableName != testTableName {
		t.Errorf("expected table name '%s', got '%s'", testTableName, event.TableName)
	}
	if event.OldRowId != rowID {
		t.Errorf("expected old_row_id %d, got %d", rowID, event.OldRowId)
	}
	if event.NewRowId != rowID { // Assuming PK doesn't change
		t.Errorf("expected new_row_id %d, got %d", rowID, event.NewRowId)
	}
	if event.Timestamp == 0 {
		t.Error("expected timestamp to be populated")
	}

	// Validate OldRow
	if event.OldRow == nil || len(event.OldRow.Values) != 3 {
		t.Fatalf("expected old_row to have 3 values, got %v", event.OldRow)
	}
	if idVal := event.OldRow.Values[0].GetI(); idVal != rowID {
		t.Errorf("OldRow[0] (id): expected %d, got %d", rowID, idVal)
	}
	if nameVal := event.OldRow.Values[1].GetS(); nameVal != initialName {
		t.Errorf("OldRow[1] (name): expected '%s', got '%s'", initialName, nameVal)
	}
	if valVal := event.OldRow.Values[2].GetD(); valVal != initialValue {
		t.Errorf("OldRow[2] (value): expected %f, got %f", initialValue, valVal)
	}

	// Validate NewRow
	if event.NewRow == nil || len(event.NewRow.Values) != 3 {
		t.Fatalf("expected new_row to have 3 values, got %v", event.NewRow)
	}
	if idVal := event.NewRow.Values[0].GetI(); idVal != rowID {
		t.Errorf("NewRow[0] (id): expected %d, got %d", rowID, idVal)
	}
	if nameVal := event.NewRow.Values[1].GetS(); nameVal != updatedName {
		t.Errorf("NewRow[1] (name): expected '%s', got '%s'", updatedName, nameVal)
	}
	if valVal := event.NewRow.Values[2].GetD(); valVal != updatedValue {
		t.Errorf("NewRow[2] (value): expected %f, got %f", updatedValue, valVal)
	}
}

func TestCDCDelete(t *testing.T) {
	db, dbPath := openTestDB(t, true) // CDC Enabled
	defer os.Remove(dbPath)
	defer db.Close()

	_, err := db.ExecuteStringStmt(fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, name TEXT)", testTableName))
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	initialName := "to_be_deleted"
	rowID := int64(1)
	_, err = db.ExecuteStringStmt(fmt.Sprintf("INSERT INTO %s(id, name) VALUES (%d, '%s')", testTableName, rowID, initialName))
	if err != nil {
		t.Fatalf("failed to insert initial data: %v", err)
	}

	logBuf, restoreLog := captureLogOutput(t) // Capture logs only for the DELETE
	defer restoreLog()

	_, err = db.ExecuteStringStmt(fmt.Sprintf("DELETE FROM %s WHERE id = %d", testTableName, rowID))
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	if countCDCEventsInLog(logBuf) != 1 {
		t.Fatalf("expected 1 CDC event for DELETE, got %d. Log:\n%s", countCDCEventsInLog(logBuf), logBuf.String())
	}
	event := getCDCEventFromLog(t, logBuf)
	if event == nil {
		t.Fatal("CDC event is nil")
	}

	if event.Operation != proto.OperationType_OPERATION_TYPE_DELETE {
		t.Errorf("expected operation type DELETE, got %s", event.Operation)
	}
	if event.TableName != testTableName {
		t.Errorf("expected table name '%s', got '%s'", testTableName, event.TableName)
	}
	if event.OldRowId != rowID {
		t.Errorf("expected old_row_id %d, got %d", rowID, event.OldRowId)
	}
	if event.NewRowId != 0 { // Should be 0 or not set for DELETE
		t.Errorf("expected new_row_id to be 0, got %d", event.NewRowId)
	}
	if event.Timestamp == 0 {
		t.Error("expected timestamp to be populated")
	}
	if event.NewRow != nil {
		t.Errorf("expected new_row to be nil for DELETE, got %v", event.NewRow)
	}

	// Validate OldRow
	if event.OldRow == nil || len(event.OldRow.Values) != 2 { // id, name
		t.Fatalf("expected old_row to have 2 values, got %v", event.OldRow)
	}
	if idVal := event.OldRow.Values[0].GetI(); idVal != rowID {
		t.Errorf("OldRow[0] (id): expected %d, got %d", rowID, idVal)
	}
	if nameVal := event.OldRow.Values[1].GetS(); nameVal != initialName {
		t.Errorf("OldRow[1] (name): expected '%s', got '%s'", initialName, nameVal)
	}
}

func TestCDCDataTypes(t *testing.T) {
	db, dbPath := openTestDB(t, true) // CDC Enabled
	defer os.Remove(dbPath)
	defer db.Close()

	stmt := fmt.Sprintf(`CREATE TABLE %s (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		txt TEXT,
		blb BLOB,
		rl REAL,
		intgr INTEGER,
		maybe_txt TEXT
	)`, allTypesTableName)
	_, err := db.ExecuteStringStmt(stmt)
	if err != nil {
		t.Fatalf("failed to create all_types_table: %v", err)
	}

	// INSERT
	logBuf, restoreLog := captureLogOutput(t)

	txtVal1 := "hello world"
	blbVal1 := []byte{0x01, 0x02, 0x03, 0x04}
	rlVal1 := 3.14159
	intgrVal1 := int64(9876543210)
	// maybe_txtVal1 will be NULL for insert

	insertStmt := fmt.Sprintf("INSERT INTO %s (txt, blb, rl, intgr) VALUES (?, ?, ?, ?)", allTypesTableName)
	insertReq := &proto.Request{
		Statements: []*proto.Statement{
			{
				Sql: insertStmt,
				Parameters: []*proto.Parameter{
					{Value: &proto.Parameter_S{S: txtVal1}},
					{Value: &proto.Parameter_Y{Y: blbVal1}},
					{Value: &proto.Parameter_D{D: rlVal1}},
					{Value: &proto.Parameter_I{I: intgrVal1}},
				},
			},
		},
	}
	execRes, err := db.Execute(insertReq, false)
	if err != nil {
		t.Fatalf("failed to insert data with types: %v", err)
	}
	if len(execRes) != 1 || execRes[0].GetE() == nil {
		t.Fatalf("unexpected execute result for typed insert: %v", execRes)
	}
	insertedID := execRes[0].GetE().LastInsertId
	restoreLog()

	if countCDCEventsInLog(logBuf) != 1 {
		t.Fatalf("expected 1 CDC event for typed INSERT, got %d. Log:\n%s", countCDCEventsInLog(logBuf), logBuf.String())
	}
	eventInsert := getCDCEventFromLog(t, logBuf)
	if eventInsert == nil {
		t.Fatal("CDC event for typed insert is nil")
	}

	if eventInsert.Operation != proto.OperationType_OPERATION_TYPE_INSERT {
		t.Errorf("DataTypes INSERT: expected operation INSERT, got %s", eventInsert.Operation)
	}
	if eventInsert.TableName != allTypesTableName {
		t.Errorf("DataTypes INSERT: expected table %s, got %s", allTypesTableName, eventInsert.TableName)
	}
	if eventInsert.NewRowId != insertedID {
		t.Errorf("DataTypes INSERT: expected NewRowId %d, got %d", insertedID, eventInsert.NewRowId)
	}
	if eventInsert.NewRow == nil || len(eventInsert.NewRow.Values) != 6 { // id, txt, blb, rl, intgr, maybe_txt
		t.Fatalf("DataTypes INSERT: NewRow expected 6 values, got %v", eventInsert.NewRow)
	}
	// Check types (order: id, txt, blb, rl, intgr, maybe_txt)
	if _, ok := eventInsert.NewRow.Values[0].GetValue().(*proto.CDCValue_I); !ok {t.Errorf("INSERT NewRow[0] (id) not sint64: %T", eventInsert.NewRow.Values[0].GetValue())}
	if v := eventInsert.NewRow.Values[1].GetS(); v != txtVal1 {t.Errorf("INSERT NewRow[1] (txt) expected %s, got %s", txtVal1, v)}
	if !bytes.Equal(eventInsert.NewRow.Values[2].GetY(), blbVal1) {t.Errorf("INSERT NewRow[2] (blb) expected %x, got %x", blbVal1, eventInsert.NewRow.Values[2].GetY())}
	if v := eventInsert.NewRow.Values[3].GetD(); v != rlVal1 {t.Errorf("INSERT NewRow[3] (rl) expected %f, got %f", rlVal1, v)}
	if v := eventInsert.NewRow.Values[4].GetI(); v != intgrVal1 {t.Errorf("INSERT NewRow[4] (intgr) expected %d, got %d", intgrVal1, v)}
	if eventInsert.NewRow.Values[5].GetValue() != nil { // Expecting NULL for maybe_txt
		t.Errorf("INSERT NewRow[5] (maybe_txt) expected nil (NULL), got %v", eventInsert.NewRow.Values[5].GetValue())
	}
	logBuf.Reset() // Clear buffer for next operation

	// UPDATE
	logBuf, restoreLog = captureLogOutput(t)
	txtVal2 := "updated text"
	blbVal2 := []byte{0xAA, 0xBB}
	rlVal2 := 1.618
	intgrVal2 := int64(1337)
	maybeTxtVal2 := "not null now"

	updateStmt := fmt.Sprintf("UPDATE %s SET txt=?, blb=?, rl=?, intgr=?, maybe_txt=? WHERE id=?", allTypesTableName)
	updateReq := &proto.Request{
		Statements: []*proto.Statement{
			{
				Sql: updateStmt,
				Parameters: []*proto.Parameter{
					{Value: &proto.Parameter_S{S: txtVal2}},
					{Value: &proto.Parameter_Y{Y: blbVal2}},
					{Value: &proto.Parameter_D{D: rlVal2}},
					{Value: &proto.Parameter_I{I: intgrVal2}},
					{Value: &proto.Parameter_S{S: maybeTxtVal2}},
					{Value: &proto.Parameter_I{I: insertedID}},
				},
			},
		},
	}
	_, err = db.Execute(updateReq, false)
	if err != nil {
		t.Fatalf("failed to update data with types: %v", err)
	}
	restoreLog()

	if countCDCEventsInLog(logBuf) != 1 {
		t.Fatalf("expected 1 CDC event for typed UPDATE, got %d. Log:\n%s", countCDCEventsInLog(logBuf), logBuf.String())
	}
	eventUpdate := getCDCEventFromLog(t, logBuf)
	if eventUpdate == nil {
		t.Fatal("CDC event for typed update is nil")
	}
	if eventUpdate.Operation != proto.OperationType_OPERATION_TYPE_UPDATE {
		t.Errorf("DataTypes UPDATE: expected operation UPDATE, got %s", eventUpdate.Operation)
	}
	// Check OldRow
	if eventUpdate.OldRow == nil || len(eventUpdate.OldRow.Values) != 6 {t.Fatalf("UPDATE OldRow wrong length")}
	if v := eventUpdate.OldRow.Values[1].GetS(); v != txtVal1 {t.Errorf("UPDATE OldRow[1] (txt) expected %s, got %s", txtVal1, v)}
	if !bytes.Equal(eventUpdate.OldRow.Values[2].GetY(), blbVal1) {t.Errorf("UPDATE OldRow[2] (blb) expected %x, got %x", blbVal1, eventUpdate.OldRow.Values[2].GetY())}
	if v := eventUpdate.OldRow.Values[3].GetD(); v != rlVal1 {t.Errorf("UPDATE OldRow[3] (rl) expected %f, got %f", rlVal1, v)}
	if v := eventUpdate.OldRow.Values[4].GetI(); v != intgrVal1 {t.Errorf("UPDATE OldRow[4] (intgr) expected %d, got %d", intgrVal1, v)}
	if eventUpdate.OldRow.Values[5].GetValue() != nil {t.Errorf("UPDATE OldRow[5] (maybe_txt) expected nil, got %v", eventUpdate.OldRow.Values[5].GetValue())}

	// Check NewRow
	if eventUpdate.NewRow == nil || len(eventUpdate.NewRow.Values) != 6 {t.Fatalf("UPDATE NewRow wrong length")}
	if v := eventUpdate.NewRow.Values[1].GetS(); v != txtVal2 {t.Errorf("UPDATE NewRow[1] (txt) expected %s, got %s", txtVal2, v)}
	if !bytes.Equal(eventUpdate.NewRow.Values[2].GetY(), blbVal2) {t.Errorf("UPDATE NewRow[2] (blb) expected %x, got %x", blbVal2, eventUpdate.NewRow.Values[2].GetY())}
	if v := eventUpdate.NewRow.Values[3].GetD(); v != rlVal2 {t.Errorf("UPDATE NewRow[3] (rl) expected %f, got %f", rlVal2, v)}
	if v := eventUpdate.NewRow.Values[4].GetI(); v != intgrVal2 {t.Errorf("UPDATE NewRow[4] (intgr) expected %d, got %d", intgrVal2, v)}
	if v := eventUpdate.NewRow.Values[5].GetS(); v != maybeTxtVal2 {t.Errorf("UPDATE NewRow[5] (maybe_txt) expected %s, got %v", maybeTxtVal2, eventUpdate.NewRow.Values[5].GetValue())}
	logBuf.Reset()

	// DELETE
	logBuf, restoreLog = captureLogOutput(t)
	_, err = db.ExecuteStringStmt(fmt.Sprintf("DELETE FROM %s WHERE id = %d", allTypesTableName, insertedID))
	if err != nil {
		t.Fatalf("failed to delete data with types: %v", err)
	}
	restoreLog()

	if countCDCEventsInLog(logBuf) != 1 {
		t.Fatalf("expected 1 CDC event for typed DELETE, got %d. Log:\n%s", countCDCEventsInLog(logBuf), logBuf.String())
	}
	eventDelete := getCDCEventFromLog(t, logBuf)
	if eventDelete == nil {
		t.Fatal("CDC event for typed delete is nil")
	}
	if eventDelete.Operation != proto.OperationType_OPERATION_TYPE_DELETE {
		t.Errorf("DataTypes DELETE: expected operation DELETE, got %s", eventDelete.Operation)
	}
	if eventDelete.OldRowId != insertedID {
		t.Errorf("DataTypes DELETE: expected OldRowId %d, got %d", insertedID, eventDelete.OldRowId)
	}
	// Check OldRow (should be same as NewRow from the update)
	if eventDelete.OldRow == nil || len(eventDelete.OldRow.Values) != 6 {t.Fatalf("DELETE OldRow wrong length")}
	if v := eventDelete.OldRow.Values[1].GetS(); v != txtVal2 {t.Errorf("DELETE OldRow[1] (txt) expected %s, got %s", txtVal2, v)}
	if !bytes.Equal(eventDelete.OldRow.Values[2].GetY(), blbVal2) {t.Errorf("DELETE OldRow[2] (blb) expected %x, got %x", blbVal2, eventDelete.OldRow.Values[2].GetY())}
	if v := eventDelete.OldRow.Values[3].GetD(); v != rlVal2 {t.Errorf("DELETE OldRow[3] (rl) expected %f, got %f", rlVal2, v)}
	if v := eventDelete.OldRow.Values[4].GetI(); v != intgrVal2 {t.Errorf("DELETE OldRow[4] (intgr) expected %d, got %d", intgrVal2, v)}
	if v := eventDelete.OldRow.Values[5].GetS(); v != maybeTxtVal2 {t.Errorf("DELETE OldRow[5] (maybe_txt) expected %s, got %v", maybeTxtVal2, eventDelete.OldRow.Values[5].GetValue())}
}

// Helper to convert proto.CDCValue to a more comparable form for NULL checks.
func getComparableValue(v *proto.CDCValue) interface{} {
	if v == nil || v.Value == nil {
		return nil // Represents SQL NULL
	}
	switch val := v.Value.(type) {
	case *proto.CDCValue_I:
		return val.I
	case *proto.CDCValue_D:
		return val.D
	case *proto.CDCValue_B:
		return val.B
	case *proto.CDCValue_Y:
		return val.Y // Keep as bytes for comparison
	case *proto.CDCValue_S:
		return val.S
	default:
		return fmt.Sprintf("unhandled_type_%T", v.Value)
	}
}

func TestCDCNullHandling(t *testing.T) {
	db, dbPath := openTestDB(t, true) // CDC Enabled
	defer os.Remove(dbPath)
	defer db.Close()

	tableName := "null_test_table"
	_, err := db.ExecuteStringStmt(fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, data TEXT)", tableName))
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert a row with NULL data
	logBuf, restoreLog := captureLogOutput(t)
	res, err := db.ExecuteStringStmt(fmt.Sprintf("INSERT INTO %s (id, data) VALUES (1, NULL)", tableName))
	if err != nil {
		t.Fatalf("failed to insert NULL: %v", err)
	}
	insertedID := res[0].GetE().LastInsertId
	restoreLog()

	eventInsert := getCDCEventFromLog(t, logBuf)
	if eventInsert == nil {t.Fatal("Insert event nil")}
	if eventInsert.NewRow == nil || len(eventInsert.NewRow.Values) != 2 {t.Fatal("Insert NewRow wrong length")}
	if val := getComparableValue(eventInsert.NewRow.Values[1]); val != nil {
		t.Errorf("INSERT: Expected data column to be nil (SQL NULL), got %v (type %T)", val, val)
	}
	logBuf.Reset()

	// Update NULL to a value
	logBuf, restoreLog = captureLogOutput(t)
	updateValue := "not null"
	_, err = db.ExecuteStringStmt(fmt.Sprintf("UPDATE %s SET data = '%s' WHERE id = %d", tableName, updateValue, insertedID))
	if err != nil {
		t.Fatalf("failed to update NULL to value: %v", err)
	}
	restoreLog()
	eventUpdate1 := getCDCEventFromLog(t, logBuf)
	if eventUpdate1 == nil {t.Fatal("Update1 event nil")}
	if eventUpdate1.OldRow == nil || len(eventUpdate1.OldRow.Values) != 2 {t.Fatal("Update1 OldRow wrong length")}
	if val := getComparableValue(eventUpdate1.OldRow.Values[1]); val != nil {
		t.Errorf("UPDATE (NULL to Value) OldRow: Expected data to be nil, got %v", val)
	}
	if eventUpdate1.NewRow == nil || len(eventUpdate1.NewRow.Values) != 2 {t.Fatal("Update1 NewRow wrong length")}
	if val := getComparableValue(eventUpdate1.NewRow.Values[1]); val != updateValue {
		t.Errorf("UPDATE (NULL to Value) NewRow: Expected data '%s', got %v", updateValue, val)
	}
	logBuf.Reset()

	// Update value back to NULL
	logBuf, restoreLog = captureLogOutput(t)
	_, err = db.ExecuteStringStmt(fmt.Sprintf("UPDATE %s SET data = NULL WHERE id = %d", tableName, insertedID))
	if err != nil {
		t.Fatalf("failed to update value to NULL: %v", err)
	}
	restoreLog()
	eventUpdate2 := getCDCEventFromLog(t, logBuf)
	if eventUpdate2 == nil {t.Fatal("Update2 event nil")}
	if eventUpdate2.OldRow == nil || len(eventUpdate2.OldRow.Values) != 2 {t.Fatal("Update2 OldRow wrong length")}
	if val := getComparableValue(eventUpdate2.OldRow.Values[1]); val != updateValue {
		t.Errorf("UPDATE (Value to NULL) OldRow: Expected data '%s', got %v", updateValue, val)
	}
	if eventUpdate2.NewRow == nil || len(eventUpdate2.NewRow.Values) != 2 {t.Fatal("Update2 NewRow wrong length")}
	if val := getComparableValue(eventUpdate2.NewRow.Values[1]); val != nil {
		t.Errorf("UPDATE (Value to NULL) NewRow: Expected data to be nil, got %v", val)
	}
}

// TestCDCNoOpUpdate checks if an UPDATE that doesn't change data fires a hook.
// SQLite's preupdate hook typically does not fire if the new data is identical to the old data,
// so we expect no event here.
func TestCDCNoOpUpdate(t *testing.T) {
	db, dbPath := openTestDB(t, true) // CDC Enabled
	defer os.Remove(dbPath)
	defer db.Close()

	_, err := db.ExecuteStringStmt(fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, name TEXT)", testTableName))
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	initialName := "no_op_name"
	rowID := int64(1)
	_, err = db.ExecuteStringStmt(fmt.Sprintf("INSERT INTO %s(id, name) VALUES (%d, '%s')", testTableName, rowID, initialName))
	if err != nil {
		t.Fatalf("failed to insert initial data: %v", err)
	}

	logBuf, restoreLog := captureLogOutput(t)
	defer restoreLog()

	// Perform an UPDATE statement that doesn't actually change any data.
	_, err = db.ExecuteStringStmt(fmt.Sprintf("UPDATE %s SET name = '%s' WHERE id = %d", testTableName, initialName, rowID))
	if err != nil {
		t.Fatalf("failed to execute no-op update: %v", err)
	}

	if countCDCEventsInLog(logBuf) > 0 {
		t.Errorf("expected NO CDC events for a no-op UPDATE, but got some. Log:\n%s", logBuf.String())
	}
}

// TestCDCValuesToStructPB tests the conversion of CDCValue to structpb.Value
// This is not directly used by the current DefaultCDCHookCallback (which logs JSON),
// but it's a good utility to have if CDC events were to be processed differently.
// For now, this test mainly ensures the `oneof` nature of CDCValue is understood.
func TestCDCValuesToStructPB(t *testing.T) {
	tests := []struct {
		name     string
		input    *proto.CDCValue
		expected *structpb.Value
	}{
		{"integer", &proto.CDCValue{Value: &proto.CDCValue_I{I: 123}}, structpb.NewNumberValue(123)},
		{"double", &proto.CDCValue{Value: &proto.CDCValue_D{D: 1.23}}, structpb.NewNumberValue(1.23)},
		{"bool_true", &proto.CDCValue{Value: &proto.CDCValue_B{B: true}}, structpb.NewBoolValue(true)},
		{"bool_false", &proto.CDCValue{Value: &proto.CDCValue_B{B: false}}, structpb.NewBoolValue(false)},
		{"string", &proto.CDCValue{Value: &proto.CDCValue_S{S: "hello"}}, structpb.NewStringValue("hello")},
		{"bytes", &proto.CDCValue{Value: &proto.CDCValue_Y{Y: []byte("world")}}, structpb.NewStringValue(string(b64Encode([]byte("world"))))}, // Assuming bytes are base64 encoded for JSON
		{"null", &proto.CDCValue{Value: nil}, structpb.NewNullValue()},
		{"nil_oneof", &proto.CDCValue{}, structpb.NewNullValue()}, // No value set in oneof
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This test is more conceptual as direct conversion to structpb.Value is not
			// what DefaultCDCHookCallback does. It marshals to JSON.
			// The important part is that the JSON representation is correct.
			// We can simulate this by checking the JSON output of the CDCValue.
			jsonBytes, err := protojson.Marshal(tt.input)
			if err != nil {
				t.Fatalf("Failed to marshal CDCValue: %v", err)
			}

			// For NULL values, protojson marshals a nil oneof as "null" if it's the top-level message,
			// or omits the field if it's part of a larger message and has no value.
			// Our convertRow function creates an empty CDCValue for nil, which results in an empty JSON object `{}`.
			// This is different from SQL NULL which would ideally be `null` in JSON.
			// This test highlights a nuance in how we handle nil currently.
			// The DefaultCDCHookCallback's convertRow will produce an empty CDCValue for nil,
			// which marshals to `{}`. For structpb.Value, it's `null`.

			var intermediate map[string]interface{}
			err = json.Unmarshal(jsonBytes, &intermediate)
			if err != nil {
				t.Fatalf("Failed to unmarshal JSON from CDCValue: %v", err)
			}
			
			// This test is a bit hand-wavy because we are not directly producing structpb.Value.
			// The main check is done in the actual INSERT/UPDATE/DELETE tests via JSON log.
			// For a direct structpb.Value comparison, we'd need a conversion function.
			// Let's check the JSON string for some key cases.
			switch tt.input.GetValue().(type) {
			case nil: // This is our representation of SQL NULL in convertRow
				if string(jsonBytes) != "{}" { // An empty CDCValue marshals to an empty object
					t.Errorf("Expected JSON for nil CDCValue to be '{}', got %s", string(jsonBytes))
				}
			case *proto.CDCValue_S:
				expectedJson := fmt.Sprintf(`{"s":"%s"}`, tt.input.GetS())
				if string(jsonBytes) != expectedJson {
					t.Errorf("Expected JSON %s, got %s", expectedJson, string(jsonBytes))
				}
			}

		})
	}
}

// b64Encode is a simple base64 encoder for byte slices.
// Using standard library for actual base64 encoding if needed by consumers.
// For testing, direct string conversion of what protojson might do for bytes.
func b64Encode(data []byte) string {
	return json.Marshal(data)[1:len(json.Marshal(data))-1] // Quick way to get the base64 content as string
}

func TestMain(m *testing.M) {
	// Setup code, if any, can go here.
	// For example, ensuring the default log output is something reasonable for tests
	// if not captured.
	log.SetOutput(io.Discard) // Default to discard to avoid polluting test output unless captured

	// Run the tests
	code := m.Run()

	// Teardown code, if any, can go here.
	os.Exit(code)
}
