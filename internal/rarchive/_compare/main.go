package main

import (
	"bytes"
	"compress/gzip"

	"github.com/rqlite/rqlite/v8/internal/rarchive/flate"
	"github.com/rqlite/rqlite/v8/internal/rarchive/zlib"
	"github.com/rqlite/rqlite/v8/testdata/chinook"
)

func main() {
	data := []byte(chinook.DB)

	zlibed, _ := zlib.Compress(data)
	flated, _ := flate.Compress(data)
	gziped := gzipCompress(data)

	println("Original size:", len(data))
	println("Zlib size:    ", len(zlibed))
	println("Flate size:   ", len(flated))
	println("Gzip size:    ", len(gziped))
}

// write a function which gzip compresses a byte slide
func gzipCompress(data []byte) []byte {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	gz.Write(data)
	gz.Close()
	return buf.Bytes()
}
