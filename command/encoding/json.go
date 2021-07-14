package encoding

import (
	"encoding/json"
	"fmt"

	"github.com/rqlite/rqlite/command"
)

// Result represents the outcome of an operation that changes rows.
type Result struct {
	LastInsertID int64   `json:"last_insert_id,omitempty"`
	RowsAffected int64   `json:"rows_affected,omitempty"`
	Error        string  `json:"error,omitempty"`
	Time         float64 `json:"time,omitempty"`
}

// Rows represents the outcome of an operation that returns query data.
type Rows struct {
	Columns []string        `json:"columns,omitempty"`
	Types   []string        `json:"types,omitempty"`
	Values  [][]interface{} `json:"values,omitempty"`
	Error   string          `json:"error,omitempty"`
	Time    float64         `json:"time,omitempty"`
}

func JSONMarshal(i interface{}) ([]byte, error) {
	switch v := i.(type) {
	case *command.ExecuteResult:
		return json.Marshal(&Result{
			LastInsertID: v.LastInsertId,
			RowsAffected: v.RowsAffected,
			Error:        v.Error,
			Time:         v.Time,
		})
	case *command.QueryRows:
		rowValues := make([]interface{}, len(v.Columns))
		rows := make([][]interface{}, 0)

		for n := range v.Values {
			vals := v.Values[n]
			if vals == nil {
				rows = append(rows, nil)
				continue
			}

			params := vals.GetParameters()
			if params == nil {
				rows = append(rows, nil)
				continue
			}

			for p := range params {
				switch w := params[p].GetValue().(type) {
				case *command.Parameter_I:
					rowValues[p] = w.I
				case *command.Parameter_D:
					rowValues[p] = w.D
				case *command.Parameter_B:
					rowValues[p] = w.B
				case *command.Parameter_Y:
					rowValues[p] = w.Y
				case *command.Parameter_S:
					rowValues[p] = w.S
				default:
					return nil, fmt.Errorf("unsupported type: %T", w)
				}
			}
		}

		return json.Marshal(&Rows{
			Columns: v.Columns,
			Types:   v.Types,
			Error:   v.Error,
			Time:    v.Time,
		})
	}

	return json.Marshal(i)
}
