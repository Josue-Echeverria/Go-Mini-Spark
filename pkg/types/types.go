package types

import "time"

type Job struct {
    ID     int
    RDD    int // RDD ID 
    Tasks  []Task
    Status string
}

type TransformationType int

const (
	MapOp TransformationType = iota
	FilterOp
	ReduceOp
	FlatMapOp
	ReduceByKeyOp
	ShuffleOp
)

type Transformation struct {
	Type     TransformationType
	FuncName string // nombre de la función
	Args     []byte // opcional si la función recibe parámetros
}

type Task struct {
	ID              int
	PartitionID     int
	Data            interface{}
	Transformations []Transformation
}

type TaskReply struct {
	ID     string
	status int
	Data   []string
}

type Partition struct {
	ID          int
	Data        interface{}
	RebuildFunc func() interface{}
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
