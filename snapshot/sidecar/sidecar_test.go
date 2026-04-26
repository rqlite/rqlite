package sidecar

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/rqlite/rqlite/v10/internal/rsum"
)

func Test_NewCastagnoli(t *testing.T) {
	s := NewCastagnoli(0x1a2b3c4d)
	if s.CRC != "1a2b3c4d" {
		t.Fatalf("CRC = %q, want %q", s.CRC, "1a2b3c4d")
	}
	if s.Type != TypeCastagnoli {
		t.Fatalf("Type = %q, want %q", s.Type, TypeCastagnoli)
	}
	if s.Disabled {
		t.Fatalf("expected disabled to be false")
	}
}

func Test_Sidecar_CRC32(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		s := &Sidecar{CRC: "1a2b3c4d", Type: TypeCastagnoli}
		got, err := s.CRC32()
		if err != nil {
			t.Fatalf("CRC32 failed: %v", err)
		}
		if got != 0x1a2b3c4d {
			t.Fatalf("CRC32 = %08x, want %08x", got, 0x1a2b3c4d)
		}
	})

	t.Run("zero", func(t *testing.T) {
		s := &Sidecar{CRC: "00000000", Type: TypeCastagnoli}
		got, err := s.CRC32()
		if err != nil {
			t.Fatalf("CRC32 failed: %v", err)
		}
		if got != 0 {
			t.Fatalf("CRC32 = %08x, want 0", got)
		}
	})

	t.Run("unknown type", func(t *testing.T) {
		s := &Sidecar{CRC: "1a2b3c4d", Type: "bogus"}
		if _, err := s.CRC32(); err == nil {
			t.Fatal("expected error for unknown type, got nil")
		}
	})

	t.Run("invalid hex", func(t *testing.T) {
		s := &Sidecar{CRC: "not-hex!", Type: TypeCastagnoli}
		if _, err := s.CRC32(); err == nil {
			t.Fatal("expected error for invalid hex, got nil")
		}
	})

	t.Run("too short", func(t *testing.T) {
		// Sscanf with %08x would happily parse "12345678" out of a 4-char
		// "1234" by zero-padding; the strict check must reject it.
		s := &Sidecar{CRC: "1234", Type: TypeCastagnoli}
		if _, err := s.CRC32(); err == nil {
			t.Fatal("expected error for too-short CRC, got nil")
		}
	})

	t.Run("empty", func(t *testing.T) {
		s := &Sidecar{CRC: "", Type: TypeCastagnoli}
		if _, err := s.CRC32(); err == nil {
			t.Fatal("expected error for empty CRC, got nil")
		}
	})

	t.Run("too long", func(t *testing.T) {
		// Sscanf with %08x would parse "12345678" and silently drop the
		// trailing "90"; the strict check must reject it.
		s := &Sidecar{CRC: "1234567890", Type: TypeCastagnoli}
		if _, err := s.CRC32(); err == nil {
			t.Fatal("expected error for too-long CRC, got nil")
		}
	})

	t.Run("eight non-hex chars", func(t *testing.T) {
		s := &Sidecar{CRC: "zzzzzzzz", Type: TypeCastagnoli}
		if _, err := s.CRC32(); err == nil {
			t.Fatal("expected error for non-hex CRC of correct length, got nil")
		}
	})
}

func Test_WriteFile_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.crc32")
	const sum uint32 = 0x1a2b3c4d

	if err := WriteFile(path, sum); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// File contents should be a JSON object with crc and type fields.
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading file: %v", err)
	}
	var raw map[string]string
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatalf("file is not valid JSON: %v (contents=%q)", err, b)
	}
	if raw["crc"] != "1a2b3c4d" {
		t.Fatalf("crc field = %q, want %q", raw["crc"], "1a2b3c4d")
	}
	if raw["type"] != "castagnoli" {
		t.Fatalf("type field = %q, want %q", raw["type"], "castagnoli")
	}

	got, err := ReadCRC32File(path)
	if err != nil {
		t.Fatalf("ReadCRC32File failed: %v", err)
	}
	if got != sum {
		t.Fatalf("ReadCRC32File = %08x, want %08x", got, sum)
	}
}

func Test_WriteFile_Zero(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.crc32")
	if err := WriteFile(path, 0); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	got, err := ReadCRC32File(path)
	if err != nil {
		t.Fatalf("ReadCRC32File failed: %v", err)
	}
	if got != 0 {
		t.Fatalf("ReadCRC32File = %08x, want 0", got)
	}
}

func Test_ReadFile_NotFound(t *testing.T) {
	if _, err := ReadFile(filepath.Join(t.TempDir(), "nonexistent")); err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func Test_ReadFile_Invalid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.crc32")
	if err := os.WriteFile(path, []byte("not-json"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFile(path); err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

func Test_ReadCRC32File_UnknownType(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.crc32")
	if err := os.WriteFile(path, []byte(`{"crc":"00000000","type":"bogus"}`), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadCRC32File(path); err == nil {
		t.Fatal("expected error for unknown type, got nil")
	}
}

func Test_CompareFile_Match(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data.bin")
	sidecarPath := dataPath + ".crc32"

	if err := os.WriteFile(dataPath, []byte("hello world"), 0644); err != nil {
		t.Fatal(err)
	}
	sum, err := rsum.CRC32(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(sidecarPath, sum); err != nil {
		t.Fatal(err)
	}

	ok, err := CompareFile(dataPath, sidecarPath)
	if err != nil {
		t.Fatalf("CompareFile failed: %v", err)
	}
	if !ok {
		t.Fatal("expected match, got mismatch")
	}
}

func Test_CompareFile_Mismatch(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data.bin")
	sidecarPath := dataPath + ".crc32"

	if err := os.WriteFile(dataPath, []byte("hello world"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(sidecarPath, 0xdeadbeef); err != nil {
		t.Fatal(err)
	}

	ok, err := CompareFile(dataPath, sidecarPath)
	if err != nil {
		t.Fatalf("CompareFile failed: %v", err)
	}
	if ok {
		t.Fatal("expected mismatch, got match")
	}
}

func Test_CompareFile_MissingSidecar(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data.bin")
	if err := os.WriteFile(dataPath, []byte("hello"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := CompareFile(dataPath, dataPath+".crc32"); err == nil {
		t.Fatal("expected error for missing sidecar, got nil")
	}
}

func Test_CompareFile_MissingData(t *testing.T) {
	dir := t.TempDir()
	sidecarPath := filepath.Join(dir, "data.bin.crc32")
	if err := WriteFile(sidecarPath, 0x12345678); err != nil {
		t.Fatal(err)
	}
	if _, err := CompareFile(filepath.Join(dir, "data.bin"), sidecarPath); err == nil {
		t.Fatal("expected error for missing data, got nil")
	}
}
