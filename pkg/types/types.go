package types

import "time"

type Job struct {
    ID     int
    RDD    int // RDD ID 
    Tasks  []Task
    Status string
}

type JobState struct {
    JobID int
    State string
}

type TransformationType int

const (
	MapOp TransformationType = iota
	FilterOp
	ReduceOp
	FlatMapOp
	ReduceByKeyOp
	ShuffleOp
	JoinOp
)

type ReadCSVArg struct {
	FilePath      string
	KeyColumn     string
}

type Transformation struct {
	Type     TransformationType
	FuncName string // nombre de la función
	Args     []byte // opcional si la función recibe parámetros
}

type Task struct {
	ID              int
	PartitionID     int
    Data            []Row 
	Transformations []Transformation
}

type TaskJoin struct {
	ID          int
    LeftRows    []Row
    RightRows   []Row
}

type TaskReply struct {
	ID     string
	status int
	Data   []Row
}

type WorkerInfo struct {
	ID       int
	Endpoint string
	Status   int
	LastSeen time.Time
}

// WorkerHeartbeatInfo es serializable para RPC
type Heartbeat struct {
	ID            int
	Status        int
	ActiveTasks   int
	Endpoint      string
	LastHeartbeat time.Time
}

type Row struct {
    Key   interface{}
    Value interface{}
}

type JoinRequest struct {
	RddID1 int
	RddID2 int
}

// JobRequest represents a batch job submission request
type JobRequest struct {
	Name        string                 `json:"name"`
	Parallelism int                    `json:"parallelism"`
	Config      map[string]interface{} `json:"config,omitempty"`
}

// JobResponse represents the response for job/topology operations
type JobResponse struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Status      string                 `json:"status"`
	Progress    float64                `json:"progress"`
	CreatedAt   string                 `json:"created_at"`
	CompletedAt string                 `json:"completed_at,omitempty"`
	Metrics     map[string]interface{} `json:"metrics,omitempty"`
	Error       string                 `json:"error,omitempty"`
}

// ResultsResponse represents the results of a completed job
type ResultsResponse struct {
	JobID  string   `json:"job_id"`
	Paths  []string `json:"paths"`
	Format string   `json:"format"`
	Size   int64    `json:"size"`
}