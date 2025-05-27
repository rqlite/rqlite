package db

import (
	"log"
	"time"

	"github.com/rqlite/go-sqlite3"
	"github.com/rqlite/rqlite/v8/command/proto"
	"google.golang.org/protobuf/encoding/protojson"
)

// DefaultCDCHookCallback is the default implementation of the SQLitePreUpdateHook.
// It converts the SQLitePreUpdateData into a CDCEvent, serializes it to JSON,
// and logs it.
func DefaultCDCHookCallback(d sqlite3.SQLitePreUpdateData) {
	event := &proto.CDCEvent{
		TableName: d.Table,
		OldRowId:  d.OldRowID,
		NewRowId:  d.NewRowID,
		Timestamp: time.Now().UnixNano(),
	}

	switch d.Op {
	case sqlite3.SQLITE_INSERT:
		event.Operation = proto.OperationType_OPERATION_TYPE_INSERT
		newRow, err := convertRow(d.New())
		if err != nil {
			log.Printf("failed to convert new row for INSERT: %v", err)
			return
		}
		event.NewRow = newRow
	case sqlite3.SQLITE_UPDATE:
		event.Operation = proto.OperationType_OPERATION_TYPE_UPDATE
		oldRow, err := convertRow(d.Old())
		if err != nil {
			log.Printf("failed to convert old row for UPDATE: %v", err)
			return
		}
		event.OldRow = oldRow
		newRow, err := convertRow(d.New())
		if err != nil {
			log.Printf("failed to convert new row for UPDATE: %v", err)
			return
		}
		event.NewRow = newRow
	case sqlite3.SQLITE_DELETE:
		event.Operation = proto.OperationType_OPERATION_TYPE_DELETE
		oldRow, err := convertRow(d.Old())
		if err != nil {
			log.Printf("failed to convert old row for DELETE: %v", err)
			return
		}
		event.OldRow = oldRow
	default:
		log.Printf("unknown operation type: %d", d.Op)
		return
	}

	jsonBytes, err := protojson.Marshal(event)
	if err != nil {
		log.Printf("failed to marshal CDCEvent to JSON: %v", err)
		return
	}
	log.Printf("CDC Event: %s", string(jsonBytes))
}

// convertRow converts a slice of interface{} representing a row into a proto.CDCRow.
func convertRow(rowData []interface{}) (*proto.CDCRow, error) {
	if rowData == nil {
		return nil, nil
	}
	row := &proto.CDCRow{
		Values: make([]*proto.CDCValue, len(rowData)),
	}
	for i, val := range rowData {
		cdcVal := &proto.CDCValue{}
		switch v := val.(type) {
		case int64:
			cdcVal.Value = &proto.CDCValue_I{I: v}
		case float64:
			cdcVal.Value = &proto.CDCValue_D{D: v}
		case bool:
			cdcVal.Value = &proto.CDCValue_B{B: v}
		case []byte:
			cdcVal.Value = &proto.CDCValue_Y{Y: v}
		case string:
			cdcVal.Value = &proto.CDCValue_S{S: v}
		case nil:
			// Represent nil as a specific type if needed, or handle as empty.
			// For now, we'll create an empty CDCValue, which means its oneof will be nil.
			// Or, decide on a specific representation for NULL, e.g. a special string or byte sequence.
			// For simplicity, leaving it as an empty CDCValue (oneof not set).
		default:
			// This case should ideally not be reached if SQLite data types are handled.
			// Consider logging an error or returning it if an unsupported type is encountered.
			log.Printf("unsupported data type in convertRow: %T for value: %v", v, v)
			// Potentially return an error here:
			// return nil, fmt.Errorf("unsupported data type: %T", v)
		}
		row.Values[i] = cdcVal
	}
	return row, nil
}
