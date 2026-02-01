package plan

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
