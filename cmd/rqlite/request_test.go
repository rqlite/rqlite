package main

import (
	"testing"
)

func TestUnifiedResult_isQueryResult(t *testing.T) {
	tests := []struct {
		name     string
		result   unifiedResult
		expected bool
	}{
		{
			name: "query result with columns",
			result: unifiedResult{
				Columns: []string{"id", "name"},
				Types:   []string{"integer", "text"},
				Values:  [][]any{{1, "test"}},
			},
			expected: true,
		},
		{
			name: "query result with columns only",
			result: unifiedResult{
				Columns: []string{"id", "name"},
			},
			expected: true,
		},
		{
			name: "query result with types only",
			result: unifiedResult{
				Types: []string{"integer", "text"},
			},
			expected: true,
		},
		{
			name: "query result with values only",
			result: unifiedResult{
				Values: [][]any{{1, "test"}},
			},
			expected: true,
		},
		{
			name: "execute result",
			result: unifiedResult{
				LastInsertID: 1,
				RowsAffected: 2,
			},
			expected: false,
		},
		{
			name:     "empty result",
			result:   unifiedResult{},
			expected: false,
		},
		{
			name: "error result",
			result: unifiedResult{
				Error: "some error",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.result.isQueryResult()
			if got != tt.expected {
				t.Errorf("isQueryResult() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestUnifiedResult_toRows(t *testing.T) {
	result := unifiedResult{
		Columns: []string{"id", "name"},
		Types:   []string{"integer", "text"},
		Values:  [][]any{{1, "test"}},
		Time:    0.001,
		Error:   "",
	}

	rows := result.toRows()

	if len(rows.Columns) != 2 || rows.Columns[0] != "id" || rows.Columns[1] != "name" {
		t.Errorf("toRows() columns = %v, want [id name]", rows.Columns)
	}
	if len(rows.Types) != 2 || rows.Types[0] != "integer" || rows.Types[1] != "text" {
		t.Errorf("toRows() types = %v, want [integer text]", rows.Types)
	}
	if len(rows.Values) != 1 || len(rows.Values[0]) != 2 {
		t.Errorf("toRows() values = %v, want [[1 test]]", rows.Values)
	}
	if rows.Time != 0.001 {
		t.Errorf("toRows() time = %v, want 0.001", rows.Time)
	}
}

func TestUnifiedResult_toResult(t *testing.T) {
	result := unifiedResult{
		LastInsertID: 1,
		RowsAffected: 2,
		Time:         0.002,
		Error:        "",
	}

	executeResult := result.toResult()

	if executeResult.LastInsertID != 1 {
		t.Errorf("toResult() LastInsertID = %v, want 1", executeResult.LastInsertID)
	}
	if executeResult.RowsAffected != 2 {
		t.Errorf("toResult() RowsAffected = %v, want 2", executeResult.RowsAffected)
	}
	if executeResult.Time != 0.002 {
		t.Errorf("toResult() time = %v, want 0.002", executeResult.Time)
	}
}

// Test the CTE case mentioned in the issue
func TestComplexQueryDetection(t *testing.T) {
	// Simulate response for CTE query
	cteResult := unifiedResult{
		Columns: []string{"x"},
		Types:   []string{"integer"},
		Values:  [][]any{{1}},
		Time:    0.001,
	}

	if !cteResult.isQueryResult() {
		t.Error("CTE query should be detected as query result, not execute result")
	}
}
