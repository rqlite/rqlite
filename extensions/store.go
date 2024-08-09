package extensions

import (
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

// InstallFromDir installs all extensions in the given directory into the store.
func (s *Store) InstallFromDir(dir string) error {
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

// InstallFromZip installs all extensions in the given zip file into the store.
func (s *Store) InstallFromZip(zipfile string) error {
	return rarchive.UnzipToDir(zipfile, s.dir)
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
	_, err = io.Copy(dstf, srcf)
	if err != nil {
		return err
	}
	return nil
}
