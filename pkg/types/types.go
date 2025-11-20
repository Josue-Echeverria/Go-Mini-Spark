package types

type Job struct {
	ID         string
	Partitions []string
	Status     int
}

type Task struct {
	ID          string
	PartitionID string
	ExecuteFunc func(data interface{}) interface{}
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