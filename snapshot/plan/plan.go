package plan

import (
	"encoding/json"
	"os"
)

// OpType represents the type of a filesystem operation.
type OpType string

const (
	// OpRename represents a file rename operation.
	OpRename OpType = "rename"

	// OpRemove represents a file removal operation.
	OpRemove OpType = "remove"

	// OpRemoveAll represents a directory removal operation.
	OpRemoveAll OpType = "remove_all"

	// OpCheckpoint represents a WAL checkpoint operation.
	OpCheckpoint OpType = "checkpoint"
)

// Operation represents a single filesystem operation.
type Operation struct {
	Type OpType `json:"type"`

	// Src is the source path for Rename, or the path to remove for Remove and RemoveAll.
	Src string `json:"src,omitempty"`

	// Dst is the destination path for Rename.
	Dst string `json:"dst,omitempty"`

	// Fields for Checkpoint
	WALs []string `json:"wals,omitempty"`
	DB   string   `json:"db,omitempty"`
}

// Plan represents an ordered sequence of filesystem operations.
type Plan struct {
	Ops []Operation `json:"ops"`
}

// New returns a new, empty Plan.
func New() *Plan {
	return &Plan{}
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
func (p *Plan) WriteToFile(path string) error {
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
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

// Visitor is the interface that must be implemented to traverse the plan.
type Visitor interface {
	Rename(src, dst string) error
	Remove(path string) error
	RemoveAll(path string) error
	Checkpoint(db string, wals []string) error
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
			err = v.Checkpoint(op.DB, op.WALs)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
