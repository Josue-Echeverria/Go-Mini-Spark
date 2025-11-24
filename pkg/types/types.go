package types

type Job struct {
	ID         string
	Partitions []string
	Status     int
}

type TransformationType int
const (
    MapOp TransformationType = iota
    FilterOp
)

type Transformation struct {
    Type TransformationType
    FuncName string        // nombre de la función
    Args     []byte        // opcional si la función recibe parámetros
}

type Task struct {
	ID          int
	PartitionID int
	Transformations []Transformation
	ExecuteFunc func(data interface{}) interface{}
}

type TaskReply struct {
	ID   string
	status int
	Data  interface{}
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
}