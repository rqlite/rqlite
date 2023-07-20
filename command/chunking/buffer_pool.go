package chunking

import (
	"bytes"
	"sync"
)

// // Define a sync.Pool to pool the buffers.
// var cBufferPool = sync.Pool{
// 	New: func() interface{} {
// 		fmt.Println("Creating new buffer")
// 		b := bytes.NewBuffer(nil)
// 		b.Grow(65 * 1024 * 1024)
// 		return b
// 	},
// }

// func GetBuffer() *bytes.Buffer {
// 	fmt.Println("Getting buffer")
// 	return cBufferPool.Get().(*bytes.Buffer)
// }

// func PutBuffer(b *bytes.Buffer) {
// 	fmt.Println("Putting buffer")
// 	b.Reset()
// 	cBufferPool.Put(b)
// }

// create a slice of bytes.Buffer objects, and a mutex to protect it
var manualBufferPool = struct {
	sync.Mutex
	bufs []*bytes.Buffer
}{
	bufs: make([]*bytes.Buffer, 0, 1),
}

// GetBuffer returns a Buffer from the pool, or creates a new one if none are available in the pool.
func GetManualBuffer() *bytes.Buffer {
	manualBufferPool.Lock()
	defer manualBufferPool.Unlock()
	n := len(manualBufferPool.bufs)
	if n == 0 {
		buf := bytes.NewBuffer(nil)
		buf.Grow(129 * 1024 * 1024)
		return buf
	}
	b := manualBufferPool.bufs[n-1]
	manualBufferPool.bufs[n-1] = nil
	manualBufferPool.bufs = manualBufferPool.bufs[:n-1]
	return b
}

// PutBuffer returns a Buffer to the pool.
func PutManualBuffer(b *bytes.Buffer) {
	manualBufferPool.Lock()
	defer manualBufferPool.Unlock()
	b.Reset()
	manualBufferPool.bufs = append(manualBufferPool.bufs, b)
}
