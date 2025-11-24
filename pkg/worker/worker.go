package worker

import (
	"Go-Mini-Spark/pkg/types"
	"log"
	"net"
	"net/rpc"
	"math/rand"
	"strings"
)

type Worker struct {
	ID        int
	Partition map[int]types.Partition
	TaskQueue []types.Task
	Status    int
	Endpoint  string
}

var FuncRegistry = map[string]interface{}{
    "ToUpper": func(s string) string {
        return strings.ToUpper(s)
    },
    "FilterLong": func(s string) bool {
        return len(s) > 10
    },
}


func NewWorker(driverAddress, address string, maxTasks int) *Worker {
	randomInt := rand.Intn(1000) // 0 <= n < 100

	return &Worker{
		ID:        randomInt, 
		Partition: make(map[int]types.Partition),
		TaskQueue: make([]types.Task, 0),
		Status:    0,
		Endpoint:  address,
	}
}

// ExecuteTask RPC method - must be exported
func (w *Worker) ExecuteTask(task types.Task, reply *types.TaskReply) {
	log.Printf("Worker %d executing task %s\n", w.ID, task.ID)
	
	data := w.Partition[task.PartitionID].Data.([]string)
	for _, t := range task.Transformations {
		switch t.Type {

		// TODO 
		case types.MapOp:
			f := FuncRegistry[t.FuncName].(func(string) string)
			newData := make([]string, 0)
			for _, elem := range data {
				newData = append(newData, f(elem))
			}
			data = newData
	
		case types.FilterOp:
			f := FuncRegistry[t.FuncName].(func(string) bool)
			newData := make([]string, 0)
			for _, elem := range data {
				if f(elem) {
					newData = append(newData, elem)
				}
			}
			data = newData
		}
	}
	
	w.Partition[task.PartitionID] = types.Partition{
		ID:   task.PartitionID,
		Data: data,
	}

	reply.Data = data
	log.Printf("Worker %d completed task %s\n", w.ID, task.ID)
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