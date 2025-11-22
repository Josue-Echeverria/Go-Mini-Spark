package main

import (
	"Go-Mini-Spark/pkg/types"
	"flag"
	"log"
	"net"
	"net/rpc"
	"strconv"
)

type Worker struct {
	ID        int
	Partition map[int]types.Partition
	TaskQueue []string
	Status    int
	Endpoint  string
}

func NewWorker(driverAddress, address string, maxTasks int) *Worker {
	return &Worker{
		ID:        1, // Will be assigned by driver
		Partition: make(map[int]types.Partition),
		TaskQueue: make([]string, 0),
		Status:    0,
		Endpoint:  address,
	}
}

// ExecuteTask RPC method - must be exported
func (w *Worker) ExecuteTask(task types.Task, reply *string) error {
	log.Printf("Worker %d executing task %s\n", w.ID, task.ID)
	*reply = "Task executed successfully"
	return nil
}

// GetStatus RPC method
func (w *Worker) GetStatus(args struct{}, reply *int) error {
	*reply = w.Status
	return nil
}

// StorePartition stores a partition on this worker
func (w *Worker) StorePartition(partitionID int, data []byte) error {
	w.Partition[partitionID] = types.Partition{
		ID:   partitionID,
		Data: data,
	}
	return nil
}

func SendResultToDriver(result string) {
}

func (w *Worker) Start(driverAddress string) {
	log.Printf("Worker %d starting and connecting to driver at %s\n", w.ID, driverAddress)

	client, err := rpc.Dial("tcp", driverAddress)
	if err != nil {
		log.Fatal("Error connecting to driver:", err)
	}

	var reply bool
	err = client.Call("Driver.RegisterWorker", *w, &reply)
	if err != nil {
		log.Fatal("Error registering with driver:", err)
	}

	if reply {
		log.Printf("Worker %d successfully registered with driver\n", w.ID)
	} else {
		log.Printf("Worker %d registration failed\n", w.ID)
	}

	client.Close()

	// Register RPC methods
	rpc.Register(w)
	listener, err := net.Listen("tcp", w.Endpoint)
	if err != nil {
		log.Fatal("Error starting worker RPC server:", err)
	}

	log.Printf("Worker %d now listening for tasks on %s\n", w.ID, w.Endpoint)
	rpc.Accept(listener)
}

func main() {
	driverAddress := flag.String("driver", "localhost:9000", "Driver address (host:port)")
	workerAddress := flag.String("address", "localhost:9100", "Worker address (host:port)")
	maxTasks := flag.Int("max-tasks", 10, "Maximum number of tasks to handle")

	flag.Parse()

	// Support positional arguments
	args := flag.Args()
	if len(args) >= 1 {
		*driverAddress = args[0]
	}
	if len(args) >= 2 {
		*workerAddress = args[1]
	}
	if len(args) >= 3 {
		if val, err := strconv.Atoi(args[2]); err == nil {
			*maxTasks = val
		}
	}

	log.Printf("Starting worker with driver=%s, address=%s, max-tasks=%d\n", *driverAddress, *workerAddress, *maxTasks)

	worker := NewWorker(*driverAddress, *workerAddress, *maxTasks)
	worker.Start(*driverAddress)
}
