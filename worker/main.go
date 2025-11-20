package main

import (
	"Go-Mini-Spark/pkg/types"
	"log"
	"net"
	"net/rpc"
)

type Worker struct {
	ID        int
	Partition map[int]types.Partition
	TaskQueue []string
	Status    int
	Endpoint  string
}

func NewWorker(masterAddress, address string, maxTasks int) *Worker {
	return &Worker{
		ID:        1, // Will be assigned by master
		Partition: make(map[int]types.Partition),
		TaskQueue: make([]string, 0),
		Status:    0,
		Endpoint:  address,
	}
}

func ExecuteTask(task types.Task) string {
	return ""
}

func StorePartition(partitionID string, data []byte) error {
	return nil
}

func SendResultToDriver(result string) {
}

func (w *Worker) Start(masterAddress string) {
	log.Printf("Worker %d starting and connecting to master at %s\n", w.ID, masterAddress)

	client, err := rpc.Dial("tcp", masterAddress)
	if err != nil {
		log.Fatal("Error connecting to master:", err)
	}

	var reply bool
	err = client.Call("Master.RegisterWorker", *w, &reply)
	if err != nil {
		log.Fatal("Error registering with master:", err)
	}

	if reply {
		log.Printf("Worker %d successfully registered with master\n", w.ID)
	} else {
		log.Printf("Worker %d registration failed\n", w.ID)
	}

	client.Close()

	rpc.Register(w)
	listener, err := net.Listen("tcp", w.Endpoint)
	if err != nil {
		log.Fatal("Error starting worker RPC server:", err)
	}

	log.Printf("Worker %d now listening for tasks on %s\n", w.ID, w.Endpoint)
	rpc.Accept(listener)
}

func main() {
	worker := NewWorker("localhost:9000", "localhost:9100", 10)
	worker.Start("localhost:9000")
}
