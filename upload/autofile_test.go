package upload

import (
	"os"
	"testing"
)

func Test_NewAutoDeleteTempFile(t *testing.T) {
	adFile, err := NewAutoDeleteFile(mustCreateTempFilename())
	if err != nil {
		t.Fatalf("NewAutoDeleteFile() failed: %v", err)
	}
	defer adFile.Close()

	if _, err := os.Stat(adFile.Name()); os.IsNotExist(err) {
		t.Fatalf("Expected file to exist: %s", adFile.Name())
	}
}

func Test_AutoDeleteFile_Name(t *testing.T) {
	name := mustCreateTempFilename()
	adFile, err := NewAutoDeleteFile(name)
	if err != nil {
		t.Fatalf("NewAutoDeleteFile() failed: %v", err)
	}
	defer adFile.Close()

	if adFile.Name() != name {
		t.Fatalf("Expected Name() to return %s, got %s", name, adFile.Name())
	}
}

func Test_AutoDeleteFile_Close(t *testing.T) {
	adFile, err := NewAutoDeleteFile(mustCreateTempFilename())
	if err != nil {
		t.Fatalf("NewAutoDeleteFile() failed: %v", err)
	}
	filename := adFile.Name()

	err = adFile.Close()
	if err != nil {
		t.Fatalf("Close() failed: %v", err)
	}

	if _, err := os.Stat(filename); !os.IsNotExist(err) {
		t.Fatalf("Expected file to be deleted after Close(): %s", filename)
	}
}

func mustCreateTempFilename() string {
	f, err := os.CreateTemp("", "autodeletefile_test")
	if err != nil {
		panic(err)
	}
	f.Close()
	return f.Name()
}
