package auto

import (
	"encoding/json"
	"testing"
)

func TestStorageType_UnmarshalJSON(t *testing.T) {
	testCases := []struct {
		name        string
		input       string
		expected    StorageType
		expectError bool
	}{
		{
			name:        "ValidS3",
			input:       `"s3"`,
			expected:    "s3",
			expectError: false,
		},
		{
			name:        "ValidGCS",
			input:       `"gcs"`,
			expected:    "gcs",
			expectError: false,
		},
		{
			name:        "InvalidType",
			input:       `"invalid"`,
			expected:    "",
			expectError: true,
		},
		{
			name:        "NonString",
			input:       `123`,
			expected:    "",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var s StorageType
			err := json.Unmarshal([]byte(tc.input), &s)

			if tc.expectError {
				if err == nil {
					t.Fatalf("Expected error, got nil")
				}
				if err != ErrUnsupportedStorageType {
					t.Fatalf("Expected ErrUnsupportedStorageType, got %v", err)
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if s != tc.expected {
				t.Errorf("Expected %s, got %s", tc.expected, s)
			}
		})
	}
}
