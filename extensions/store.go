package extensions

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/rqlite/rqlite/v8/rarchive"
)

// Store is a collection of extensions.
type Store struct {
	dir string
}

// NewStore creates a new extension store at the given directory.
// The directory is created if it does not exist, and any existing
// files are removed.
func NewStore(dir string) (*Store, error) {
	if err := os.RemoveAll(dir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return &Store{
		dir: dir,
	}, nil
}

// Dir returns the directory of the store.
func (s *Store) Dir() string {
	return s.dir
}

// List returns a list of all file paths to extensions in the store.
func (s *Store) List() ([]string, error) {
	files, err := listFiles(s.dir)
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}

// Names returns a list of all extension names in the store.
func (s *Store) Names() ([]string, error) {
	files, err := s.List()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0)
	for _, file := range files {
		names = append(names, filepath.Base(file))
	}
	sort.Strings(names)
	return names, nil
}

// LoadFromFile installs a single extension at the given file path into the store.
func (s *Store) LoadFromFile(file string) error {
	dst := filepath.Join(s.dir, filepath.Base(file))
	return copyFile(file, dst)
}

// LoadFromDir installs all extensions in the given directory into the store.
func (s *Store) LoadFromDir(dir string) error {
	srcfiles, err := listFiles(dir)
	if err != nil {
		return err
	}
	for _, src := range srcfiles {
		dst := filepath.Join(s.dir, filepath.Base(src))
		if err := copyFile(src, dst); err != nil {
			return err
		}
	}
	return nil
}

// LoadFromZip installs all extensions in the given zip file into the store.
func (s *Store) LoadFromZip(zipfile string) error {
	h, err := rarchive.ZipHasSubdirectories(zipfile)
	if err != nil {
		return err
	}
	if h {
		return fmt.Errorf("zip file contains subdirectories")
	}
	return rarchive.UnzipToDir(zipfile, s.dir)
}

// LoadFromTarGzip installs all extensions in the given tar.gz file into the store.s
func (s *Store) LoadFromTarGzip(targzfile string) error {
	h, err := rarchive.TarGzipHasSubdirectories(targzfile)
	if err != nil {
		return err
	}
	if h {
		return fmt.Errorf("tar.gz file contains subdirectories")
	}
	return rarchive.UntarGzipToDir(targzfile, s.dir)
}

// Stats returns status and diagnostics for the Extension store.
func (s *Store) Stats() (map[string]interface{}, error) {
	stats := make(map[string]interface{})
	stats["dir"] = s.dir
	names, err := s.Names()
	if err != nil {
		return nil, err
	}
	stats["names"] = names
	return stats, nil
}

func listFiles(dir string) ([]string, error) {
	paths := make([]string, 0)
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if !f.IsDir() && !strings.HasPrefix(f.Name(), ".") {
			dstPath := filepath.Join(dir, f.Name())
			paths = append(paths, dstPath)
		}
	}
	return paths, nil
}

func copyFile(src, dst string) error {
	srcf, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcf.Close()
	dstf, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstf.Close()
	_, err = io.Copy(dstf, srcf)
	if err != nil {
		return err
	}
	return nil
}
