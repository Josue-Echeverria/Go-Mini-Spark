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