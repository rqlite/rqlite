package plan

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
)

// OpType represents the type of a snapshot store operation.
type OpType string

const (
	// OpRename represents a file or directory rename operation.
	OpRename OpType = "rename"

	// OpRemove represents a file removal operation.
	OpRemove OpType = "remove"

	// OpRemoveAll represents a recursive directory removal operation.
	OpRemoveAll OpType = "remove_all"

	// OpCheckpoint represents a WAL checkpoint operation.
	OpCheckpoint OpType = "checkpoint"

	// OpWriteMeta represents writing snapshot metadata to a directory.
	OpWriteMeta OpType = "write_meta"

	// OpMkdirAll represents a recursive directory creation operation.
	OpMkdirAll OpType = "mkdir_all"

	// OpCopyFile represents a file copy operation.
	OpCopyFile OpType = "copy_file"
)

// Operation represents a single snapshot store operation.
type Operation struct {
	Type OpType `json:"type"`

	// Src is the source path for Rename, or the path to remove for Remove and RemoveAll.
	Src string `json:"src,omitempty"`

	// Dst is the destination path for Rename, or the directory for WriteMeta.
	Dst string `json:"dst,omitempty"`

	// Fields for Checkpoint
	WALs []string `json:"wals,omitempty"`
	DB   string   `json:"db,omitempty"`

	// Data holds the raw content for WriteMeta.
	Data json.RawMessage `json:"data,omitempty"`
}

// Plan represents an ordered sequence of operations on a snapshot store.
// It is the unit of crash-safe, idempotent execution for destructive
// maintenance operations such as reaping.
type Plan struct {
	Ops           []Operation `json:"ops"`
	NReaped       int         `json:"n_reaped,omitempty"`
	NCheckpointed int         `json:"n_checkpointed,omitempty"`
}

// New returns a new, empty Plan.
func New() *Plan {
	return &Plan{Ops: []Operation{}}
}

// ReadFromFile reads a plan from the specified file.
func ReadFromFile(path string) (*Plan, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	p := New()
	if err := json.Unmarshal(data, p); err != nil {
		return nil, err
	}
	return p, nil
}

// WriteToFile writes the plan to the specified file in JSON format.
// The write is atomic: data is first written to a temporary file
// (path + ".tmp"), synced, and then renamed to the final path.
func WriteToFile(p *Plan, path string) error {
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	if err := syncFileMaybe(tmpPath); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

// Len returns the number of operations in the plan.
func (p *Plan) Len() int {
	return len(p.Ops)
}

// AddRename adds a rename operation to the plan.
func (p *Plan) AddRename(src, dst string) {
	p.Ops = append(p.Ops, Operation{
		Type: OpRename,
		Src:  src,
		Dst:  dst,
	})
}

// AddRemove adds a remove operation to the plan.
func (p *Plan) AddRemove(path string) {
	p.Ops = append(p.Ops, Operation{
		Type: OpRemove,
		Src:  path,
	})
}

// AddRemoveAll adds a remove all operation to the plan.
func (p *Plan) AddRemoveAll(path string) {
	p.Ops = append(p.Ops, Operation{
		Type: OpRemoveAll,
		Src:  path,
	})
}

// AddCheckpoint adds a checkpoint operation to the plan.
func (p *Plan) AddCheckpoint(db string, wals []string) {
	p.Ops = append(p.Ops, Operation{
		Type: OpCheckpoint,
		DB:   db,
		WALs: wals,
	})
}

// AddWriteMeta adds a metadata write operation to the plan. The data
// is stored as raw JSON so the plan package remains decoupled from the
// concrete metadata type.
func (p *Plan) AddWriteMeta(dir string, data []byte) {
	p.Ops = append(p.Ops, Operation{
		Type: OpWriteMeta,
		Dst:  dir,
		Data: json.RawMessage(data),
	})
}

// AddMkdirAll adds a recursive directory creation operation to the plan.
func (p *Plan) AddMkdirAll(path string) {
	p.Ops = append(p.Ops, Operation{
		Type: OpMkdirAll,
		Dst:  path,
	})
}

// AddCopyFile adds a file copy operation to the plan.
func (p *Plan) AddCopyFile(src, dst string) {
	p.Ops = append(p.Ops, Operation{
		Type: OpCopyFile,
		Src:  src,
		Dst:  dst,
	})
}

// Visitor is the interface that must be implemented to execute a plan.
type Visitor interface {
	Rename(src, dst string) error
	Remove(path string) error
	RemoveAll(path string) error
	Checkpoint(db string, wals []string) (int, error)
	WriteMeta(dir string, data []byte) error
	MkdirAll(path string) error
	CopyFile(src, dst string) error
}

// Execute traverses the plan, calling the appropriate method on the visitor for each operation.
// It stops and returns the first error encountered.
func (p *Plan) Execute(v Visitor) error {
	for _, op := range p.Ops {
		var err error
		switch op.Type {
		case OpRename:
			err = v.Rename(op.Src, op.Dst)
		case OpRemove:
			err = v.Remove(op.Src)
		case OpRemoveAll:
			err = v.RemoveAll(op.Src)
		case OpCheckpoint:
			_, err = v.Checkpoint(op.DB, op.WALs)
		case OpWriteMeta:
			err = v.WriteMeta(op.Dst, op.Data)
		case OpMkdirAll:
			err = v.MkdirAll(op.Dst)
		case OpCopyFile:
			err = v.CopyFile(op.Src, op.Dst)
		default:
			err = fmt.Errorf("unknown operation type: %s", op.Type)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// syncFile syncs the given file to disk. Skipped on Windows as it is
// not supported.
func syncFileMaybe(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	fd, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fd.Close()
	return fd.Sync()
}
