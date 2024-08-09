package rarchive

import (
	"archive/zip"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// IsZipFile checks if the file at the given path is a ZIP file
// by verifying the magic number (the first few bytes of the file).
func IsZipFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()

	// Read the first 4 bytes of the file to check the magic number
	buf := make([]byte, 4)
	_, err = f.Read(buf)
	if err != nil {
		return false
	}

	// ZIP files start with "PK\x03\x04"
	return bytes.Equal(buf, []byte("PK\x03\x04"))
}

// UnzipToDir decompresses the ZIP file at the given path into the specified directory.
func UnzipToDir(path, dir string) error {
	// Open the ZIP file
	r, err := zip.OpenReader(path)
	if err != nil {
		return err
	}
	defer r.Close()

	// Ensure the output directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Iterate through each file in the ZIP archive
	for _, f := range r.File {
		// Construct the full path for the file
		fpath := filepath.Join(dir, f.Name)

		// Check for directory traversal vulnerability
		if !strings.HasPrefix(fpath, filepath.Clean(dir)+string(os.PathSeparator)) {
			return errors.New("invalid file path")
		}

		if f.FileInfo().IsDir() {
			// Create the directory
			if err := os.MkdirAll(fpath, f.Mode()); err != nil {
				return err
			}
		} else {
			// Create the file
			if err := os.MkdirAll(filepath.Dir(fpath), f.Mode()); err != nil {
				return err
			}

			outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
			if err != nil {
				return err
			}

			rc, err := f.Open()
			if err != nil {
				return err
			}

			_, err = io.Copy(outFile, rc)

			// Close the file handles
			outFile.Close()
			rc.Close()

			if err != nil {
				return err
			}
		}
	}

	return nil
}
