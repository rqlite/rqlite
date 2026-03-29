package snapshot

import (
	"fmt"
	"sync"
)

// CRCChecker verifies the CRC32 integrity of a set of ChecksummedFiles
// in parallel.
type CRCChecker struct {
	files []*ChecksummedFile
}

// NewCRCChecker returns a new CRCChecker.
func NewCRCChecker() *CRCChecker {
	return &CRCChecker{}
}

// Add adds a ChecksummedFile to the checker.
func (c *CRCChecker) Add(hf *ChecksummedFile) {
	c.files = append(c.files, hf)
}

// Check verifies all added files in parallel. The returned channel receives
// nil if all checks pass, or the first error encountered.
func (c *CRCChecker) Check() <-chan error {
	errCh := make(chan error, 1)

	go func() {
		defer close(errCh)

		var wg sync.WaitGroup
		innerCh := make(chan error, 1)

		for _, hf := range c.files {
			wg.Go(func() {
				ok, err := hf.Check()
				if err != nil {
					select {
					case innerCh <- fmt.Errorf("CRC32 check of %s: %w", hf.Path, err):
					default:
					}
					return
				}
				if !ok {
					select {
					case innerCh <- fmt.Errorf("CRC32 mismatch for %s", hf.Path):
					default:
					}
				}
			})
		}
		wg.Wait()
		close(innerCh)

		errCh <- <-innerCh
	}()

	return errCh
}
