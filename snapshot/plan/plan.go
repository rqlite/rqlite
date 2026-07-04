package plan

import (
	"encoding/json"
	"expvar"
	"fmt"
	"os"
	"runtime"
	"time"
)

const (
	checkpointDuration = "checkpoint_duration_ms"
	calcCRC32Duration  = "calc_crc32_duration_ms"
)

// stats captures stats for the Store.
var stats *expvar.Map

func init() {
	stats = expvar.NewMap("snapshot.plan")
	ResetStats()
}

func recordDuration(stat string, startT time.Time) {
	stats.Get(stat).(*expvar.Int).Set(time.Since(startT).Milliseconds())
}

// ResetStats resets the expvar stats for this module. Mostly for test purposes.
func ResetStats() {
	stats.Init()
	stats.Add(checkpointDuration, 0)
	stats.Add(calcCRC32Duration, 0)
}

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

	// OpCalcCRC32 represents a CRC32 checksum calculation operation.
	// Src is the data file to checksum; Dst is the path for the CRC sidecar file.
	OpCalcCRC32 OpType = "calc_crc32"

	// OpVerifyDB represents running a databse integrity check.
	// Src is the database to check.
	OpVerifyDB OpType = "verify_db"
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

// AddCalcCRC32 adds a CRC32 checksum calculation operation to the plan.
// dataPath is the file to checksum; crcPath is the sidecar file to write.
func (p *Plan) AddCalcCRC32(dataPath, crcPath string) {
	p.Ops = append(p.Ops, Operation{
		Type: OpCalcCRC32,
		Src:  dataPath,
		Dst:  crcPath,
	})
}

// AddVerifyDB adds an integrity check of the database at path.
func (p *Plan) AddVerifyDB(path string) {
	p.Ops = append(p.Ops, Operation{
		Type: OpVerifyDB,
		Src:  path,
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
	CalcCRC32(dataPath, crcPath string) error
	VerifyDB(db string) error
}

// Inspector reports, for a single operation, whether that operation's effect is
// already present on disk -- i.e. whether the operation has already been
// applied. It is the read-only, query counterpart to Visitor: Visitor performs
// operations, an Inspector inspects whether they are done. It is used to decide
// whether an interrupted plan has already run to completion before replaying it.
// The concrete implementation is Checker.
type Inspector interface {
	RenameDone(src, dst string) (bool, error)
	RemoveDone(path string) (bool, error)
	RemoveAllDone(path string) (bool, error)
	CheckpointDone(db string, wals []string) (bool, error)
	WriteMetaDone(dir string, data []byte) (bool, error)
	MkdirAllDone(path string) (bool, error)
	CopyFileDone(src, dst string) (bool, error)
	CalcCRC32Done(dataPath, crcPath string) (bool, error)
	VerifyDBDone(db string) (bool, error)
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
		case OpCalcCRC32:
			err = v.CalcCRC32(op.Src, op.Dst)
		case OpVerifyDB:
			err = v.VerifyDB(op.Src)
		default:
			err = fmt.Errorf("unknown operation type: %s", op.Type)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// LastOpDone reports whether the last operation in the plan has already been
// applied, by dispatching that operation to the matching Inspector method. This
// mirrors Execute's dispatch, but inspects a single operation instead of
// performing all of them.
//
// Ideally plan-execution is idempotent and if a plan is found post-restart-after-crash
// the system can just execute the plan again. However this allows the system to ask
// directly "is the plan executed". Certain recovery scenarios post crash may need
// this option.
//
// An empty plan has no work to do and is reported as done.
func (p *Plan) LastOpDone(c Inspector) (bool, error) {
	if len(p.Ops) == 0 {
		return true, nil
	}
	op := p.Ops[len(p.Ops)-1]
	switch op.Type {
	case OpRename:
		return c.RenameDone(op.Src, op.Dst)
	case OpRemove:
		return c.RemoveDone(op.Src)
	case OpRemoveAll:
		return c.RemoveAllDone(op.Src)
	case OpCheckpoint:
		return c.CheckpointDone(op.DB, op.WALs)
	case OpWriteMeta:
		return c.WriteMetaDone(op.Dst, op.Data)
	case OpMkdirAll:
		return c.MkdirAllDone(op.Dst)
	case OpCopyFile:
		return c.CopyFileDone(op.Src, op.Dst)
	case OpCalcCRC32:
		return c.CalcCRC32Done(op.Src, op.Dst)
	case OpVerifyDB:
		return c.VerifyDBDone(op.Src)
	default:
		return false, fmt.Errorf("unknown operation type: %s", op.Type)
	}
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
