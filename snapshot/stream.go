package snapshot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"google.golang.org/protobuf/proto"
)

const strHeaderLenSize = 8

func NewStreamHeader() *StreamHeader {
	return &StreamHeader{
		Version: 1,
	}
}

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

type Stream struct {
	headerLen int64

	readClosers    []io.ReadCloser
	readClosersIdx int
	totalFileSize  int64
}

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
	return &Stream{
		headerLen:   int64(len(strHdrPb)),
		readClosers: []io.ReadCloser{io.NopCloser(bytes.NewBuffer(buf))},
	}, nil
}

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

	var readClosers []io.ReadCloser
	readClosers = append(readClosers, io.NopCloser(bytes.NewBuffer(buf)))
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

func (s *Stream) Size() int64 {
	return int64(strHeaderLenSize + int64(s.headerLen) + s.totalFileSize)
}

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

func (s *Stream) Close() error {
	for _, r := range s.readClosers {
		if err := r.Close(); err != nil {
			return err
		}
	}
	return nil
}
