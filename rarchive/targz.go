package rarchive

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// IsTarGzipFile checks if the file at the given path is a gzipped tarball
// by attempting to open it as such.
func IsTarGzipFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()

	// Attempt to create a gzip reader
	gzr, err := gzip.NewReader(f)
	if err != nil {
		return false
	}
	defer gzr.Close()

	return true
}

// TarGzipHasSubdirectories checks if the gzipped tarball contains any entries that
// represent subdirectories or files within subdirectories.
func TarGzipHasSubdirectories(path string) (bool, error) {
	// Open the tar.gz file
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer file.Close()

	gzr, err := gzip.NewReader(file)
	if err != nil {
		return false, err
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)

	// Iterate through each file in the tar archive
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break // End of tar archive
		}
		if err != nil {
			return false, err
		}

		// Check if the file is a directory or if the file path includes a subdirectory
		if header.Typeflag == tar.TypeDir ||
			strings.Contains(header.Name, "/") || strings.Contains(header.Name, "\\") {
			return true, nil
		}
	}
	return false, nil
}

// UntarGzipToDir decompresses the gzipped tarball at the given path into the specified directory.
func UntarGzipToDir(path, dir string) error {
	// Open the tar.gz file
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	gzr, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)

	// Ensure the output directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Iterate through each file in the tar archive
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break // End of tar archive
		}
		if err != nil {
			return err
		}

		// Construct the full path for the file
		fpath := filepath.Join(dir, header.Name)

		// Check for directory traversal vulnerability
		if !strings.HasPrefix(fpath, filepath.Clean(dir)+string(os.PathSeparator)) {
			return errors.New("invalid file path")
		}

		if header.Typeflag == tar.TypeDir {
			// Create the directory
			if err := os.MkdirAll(fpath, os.FileMode(header.Mode)); err != nil {
				return err
			}
		} else {
			// Create the file
			if err := os.MkdirAll(filepath.Dir(fpath), os.FileMode(header.Mode)); err != nil {
				return err
			}

			outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.FileMode(header.Mode))
			if err != nil {
				return err
			}

			// Copy the file content to the destination
			if _, err := io.Copy(outFile, tr); err != nil {
				outFile.Close()
				return err
			}

			outFile.Close()
		}
	}

	return nil
}
