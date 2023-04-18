package http

import (
	"errors"
	"reflect"
	"testing"
)

func Test_BackupHTTPRequest_Validate(t *testing.T) {
	tests := []struct {
		name    string
		input   *BackupHTTPRequest
		wantErr error
	}{
		{
			name: "valid POST request",
			input: &BackupHTTPRequest{
				URL:    "https://www.example.com",
				Method: "POST",
			},
			wantErr: nil,
		},
		{
			name: "valid PUT request",
			input: &BackupHTTPRequest{
				URL:    "https://www.example.com",
				Method: "PUT",
			},
			wantErr: nil,
		},
		{
			name: "valid request with empty method",
			input: &BackupHTTPRequest{
				URL:    "https://www.example.com",
				Method: "",
			},
			wantErr: nil,
		},
		{
			name: "invalid URL",
			input: &BackupHTTPRequest{
				URL:    "invalid_url",
				Method: "POST",
			},
			wantErr: errors.New("invalid URL"),
		},
		{
			name: "invalid method",
			input: &BackupHTTPRequest{
				URL:    "https://www.example.com",
				Method: "INVALID",
			},
			wantErr: errors.New("invalid method"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.input.Validate()
			if (err != nil && tt.wantErr == nil) || (err == nil && tt.wantErr != nil) || (err != nil && tt.wantErr != nil && err.Error() != tt.wantErr.Error()) {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_ParseBackupHTTPRequest(t *testing.T) {
	tests := []struct {
		name                 string
		input                []byte
		expBackupHTTPRequest *BackupHTTPRequest
		expErr               error
	}{
		{
			name: "valid JSON input",
			input: []byte(`{
				"url": "https://www.example.com",
				"headers": {
					"xxx": "yyy",
					"zzz": "aaa"
				},
				"method": "PUT"
			}`),
			expBackupHTTPRequest: &BackupHTTPRequest{
				URL: "https://www.example.com",
				Headers: map[string]string{
					"xxx": "yyy",
					"zzz": "aaa",
				},
				Method: "PUT",
			},
			expErr: nil,
		},
		{
			name: "valid JSON input with empty method",
			input: []byte(`{
				"url": "https://www.example.com",
				"headers": {
					"xxx": "yyy",
					"zzz": "aaa"
				}
			}`),
			expBackupHTTPRequest: &BackupHTTPRequest{
				URL: "https://www.example.com",
				Headers: map[string]string{
					"xxx": "yyy",
					"zzz": "aaa",
				},
				Method: "POST",
			},
			expErr: nil,
		},
		{
			name: "invalid JSON input",
			input: []byte(`{
				"url": "invalid_url",
				"headers": {
					"xxx": "yyy",
					"zzz": "aaa"
				},
				"method": "PUT"
			}`),
			expBackupHTTPRequest: nil,
			expErr:               errors.New("invalid URL"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bhr, err := ParseBackupHTTPRequest(tt.input)
			if !reflect.DeepEqual(bhr, tt.expBackupHTTPRequest) {
				t.Errorf("ParseBackupHTTPRequest() = %v, want %v", bhr, tt.expBackupHTTPRequest)
			}
			if (err != nil && tt.expErr == nil) || (err == nil && tt.expErr != nil) || (err != nil && tt.expErr != nil && err.Error() != tt.expErr.Error()) {
				t.Errorf("ParseBackupHTTPRequest() error = %v, expErr %v", err, tt.expErr)
			}
		})
	}
}
