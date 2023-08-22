package snapshot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"google.golang.org/protobuf/proto"
)

const (
	strHeaderLenSize = 8
	streamVersion    = 1
)

// NewStreamHeader creates a new StreamHeader.
func NewStreamHeader() *StreamHeader {
	return &StreamHeader{
		Version: streamVersion,
	}
}

// NewStreamHeaderFromReader reads a StreamHeader from the given reader.
func NewStreamHeaderFromReader(r io.Reader) (*StreamHeader, int64, error) {
	var totalSizeRead int64

	b := make([]byte, strHeaderLenSize)
	n, err := io.ReadFull(r, b)
	if err != nil {
		return nil, 0, fmt.Errorf("error reading snapshot header length: %v", err)
	}
	totalSizeRead += int64(n)
	strHdrLen := binary.LittleEndian.Uint64(b)

	b = make([]byte, strHdrLen)
	n, err = io.ReadFull(r, b)
	if err != nil {
		return nil, 0, fmt.Errorf("error reading snapshot header %v", err)
	}
	totalSizeRead += int64(n)

	strHdr := &StreamHeader{}
	err = proto.Unmarshal(b, strHdr)
	if err != nil {
		return nil, 0, fmt.Errorf("error unmarshaling FSM snapshot: %v", err)
	}
	if strHdr.GetVersion() != streamVersion {
		return nil, 0, fmt.Errorf("unsupported snapshot version %d", strHdr.GetVersion())
	}
	return strHdr, totalSizeRead, nil
}

// FileSize returns the total size of the files in the snapshot.
func (s *StreamHeader) FileSize() int64 {
	if fs := s.GetFullSnapshot(); fs != nil {
		var size int64
		for _, di := range fs.Wals {
			size += di.Size
		}
		size += fs.Db.Size
		return size
	}
	return 0
}

// Stream is a stream of data that can be read from a snapshot.
type Stream struct {
	headerLen int64

	readClosers    []io.ReadCloser
	readClosersIdx int
	totalFileSize  int64
}

// NewIncrementalStream creates a new stream from a byte slice, presumably
// representing WAL data.
func NewIncrementalStream(data []byte) (*Stream, error) {
	strHdr := NewStreamHeader()
	strHdr.Payload = &StreamHeader_IncrementalSnapshot{
		IncrementalSnapshot: &IncrementalSnapshot{
			Data: data,
		},
	}
	strHdrPb, err := proto.Marshal(strHdr)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, strHeaderLenSize)
	binary.LittleEndian.PutUint64(buf, uint64(len(strHdrPb)))
	buf = append(buf, strHdrPb...)
	return &Stream{
		headerLen:   int64(len(strHdrPb)),
		readClosers: []io.ReadCloser{newRCBuffer(buf)},
	}, nil
}

// NewFullStream creates a new stream from a SQLite file and 0 or more
// WAL files.
func NewFullStream(files ...string) (*Stream, error) {
	if len(files) == 0 {
		return nil, errors.New("no files provided")
	}

	var totalFileSize int64
	// First file must be the SQLite database file.
	fi, err := os.Stat(files[0])
	if err != nil {
		return nil, err
	}
	dbDataInfo := &FullSnapshot_DataInfo{
		Size: fi.Size(),
	}
	totalFileSize += fi.Size()

	// Rest, if any, are WAL files.
	walDataInfos := make([]*FullSnapshot_DataInfo, len(files)-1)
	for i := 1; i < len(files); i++ {
		fi, err := os.Stat(files[i])
		if err != nil {
			return nil, err
		}
		walDataInfos[i-1] = &FullSnapshot_DataInfo{
			Size: fi.Size(),
		}
		totalFileSize += fi.Size()
	}
	strHdr := NewStreamHeader()
	strHdr.Payload = &StreamHeader_FullSnapshot{
		FullSnapshot: &FullSnapshot{
			Db:   dbDataInfo,
			Wals: walDataInfos,
		},
	}

	strHdrPb, err := proto.Marshal(strHdr)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, strHeaderLenSize)
	binary.LittleEndian.PutUint64(buf, uint64(len(strHdrPb)))
	buf = append(buf, strHdrPb...)

	var readClosers []io.ReadCloser
	readClosers = append(readClosers, newRCBuffer(buf))
	for _, file := range files {
		fd, err := os.Open(file)
		if err != nil {
			for _, rc := range readClosers {
				rc.Close() // Ignore the error during cleanup
			}
			return nil, err
		}
		readClosers = append(readClosers, fd)
	}
	return &Stream{
		headerLen:     int64(len(strHdrPb)),
		readClosers:   readClosers,
		totalFileSize: strHdr.FileSize(),
	}, nil
}

// Size returns the total number of bytes that will be read from the stream,
// if the stream is fully read.
func (s *Stream) Size() int64 {
	return int64(strHeaderLenSize + int64(s.headerLen) + s.totalFileSize)
}

// Read reads from the stream.
func (s *Stream) Read(p []byte) (n int, err error) {
	if s.readClosersIdx >= len(s.readClosers) {
		return 0, io.EOF
	}

	n, err = s.readClosers[s.readClosersIdx].Read(p)
	if err != nil {
		if err == io.EOF {
			s.readClosersIdx++
			if s.readClosersIdx < len(s.readClosers) {
				err = nil
			}
		}
	}
	return n, err
}

// Close closes the stream.
func (s *Stream) Close() error {
	for _, r := range s.readClosers {
		if err := r.Close(); err != nil {
			return err
		}
	}
	return nil
}

// FilesFromStream reads a stream and returns the files contained within it.
// The first file is the SQLite database file, and the rest are WAL files.
// The function will return an error if the stream does not contain a
// FullSnapshot.
func FilesFromStream(r io.Reader) (string, []string, error) {
	strHdr, _, err := NewStreamHeaderFromReader(r)
	if err != nil {
		return "", nil, fmt.Errorf("error reading stream header: %v", err)
	}

	fullSnap := strHdr.GetFullSnapshot()
	if fullSnap == nil {
		return "", nil, fmt.Errorf("got nil FullSnapshot")
	}
	dbInfo := fullSnap.GetDb()
	if dbInfo == nil {
		return "", nil, fmt.Errorf("got nil DB info")
	}

	sqliteFd, err := os.CreateTemp("", "stream-db.sqlite3")
	if _, err := io.CopyN(sqliteFd, r, dbInfo.Size); err != nil {
		return "", nil, fmt.Errorf("error writing SQLite file data: %v", err)
	}
	if sqliteFd.Close(); err != nil {
		return "", nil, fmt.Errorf("error closing SQLite data file %v", err)
	}

	var walFiles []string
	for i, di := range fullSnap.Wals {
		tmpFd, err := os.CreateTemp("", fmt.Sprintf("stream-wal-%d.wal", i))
		if err != nil {
			return "", nil, fmt.Errorf("error creating WAL file: %v", err)
		}
		if _, err := io.CopyN(tmpFd, r, di.Size); err != nil {
			return "", nil, fmt.Errorf("error writing WAL file data: %v", err)
		}
		if err := tmpFd.Close(); err != nil {
			return "", nil, fmt.Errorf("error closing WAL file: %v", err)
		}
		walFiles = append(walFiles, tmpFd.Name())
	}
	return sqliteFd.Name(), walFiles, nil
}

func newRCBuffer(b []byte) io.ReadCloser {
	return io.NopCloser(bytes.NewBuffer(b))
}
