package main

import (
	"Go-Mini-Spark/pkg/types"
	"log"
	"net"
	"net/rpc"
)

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

type Master struct {
	Workers      map[int]types.WorkerInfo
	Jobs         map[string]*Job
	Tasks        map[string]*Task
	PartitionMap map[string][]string
	Port         string
}

func NewMaster(port string) *Master {
	return &Master{
		Workers:      make(map[int]types.WorkerInfo),
		Jobs:         make(map[string]*Job),
		Tasks:        make(map[string]*Task),
		PartitionMap: make(map[string][]string),
		Port:         port,
	}
}

func runJob() {
}

func assignTask(partitionID string, workerID string) {
}

func (d *Master) RegisterWorker(info types.WorkerInfo, reply *bool) error {
	d.Workers[info.ID] = info
	log.Printf("Registered worker %d at %s\n", info.ID, info.Endpoint)
	*reply = true
	return nil
}

func registerRDD() {
}

func (m *Master) Start() {
	log.Printf("Master server starting on port %s\n", m.Port)

	rpc.Register(m)
	listener, err := net.Listen("tcp", ":"+m.Port)
	if err != nil {
		log.Fatal("Error starting master:", err)
	}

	log.Printf("Master listening on port %s\n", m.Port)
	rpc.Accept(listener)
}

func main() {
	master := NewMaster("9000")
	master.Start()
}
