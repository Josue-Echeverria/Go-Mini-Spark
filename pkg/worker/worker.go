package worker

import (
	"Go-Mini-Spark/pkg/types"
	"Go-Mini-Spark/pkg/utils"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"strings"
	"time"
)

const heartBeatInterval = 5

type Worker struct {
	ID            int
	Partition     map[int][]string
	TaskQueue     []types.Task
	Status        int
	Endpoint      string
	DriverAddress string
	LastHeartbeat time.Time
	ActiveTasks   int
}

var FuncRegistry = map[string]interface{}{
	"ToUpper": func(s string) string { return strings.ToUpper(s) },
	"IsLong":  func(s string) bool { return len(s) > 10 },
	"SplitWords": func(s string) []string {
		return strings.Split(s, " ")
	},
	"Concat": func(a, b string) string {
		return a + b
	},
}

func NewWorker(driverAddress, address string, maxTasks int) *Worker {
	randomInt := rand.Intn(1000) // 0 <= n < 100

	return &Worker{
		ID:            randomInt,
		Partition:     make(map[int][]string),
		TaskQueue:     make([]types.Task, 0),
		Status:        0,
		Endpoint:      address,
		DriverAddress: driverAddress,
		LastHeartbeat: time.Now(),
		ActiveTasks:   0,
	}
}

func ExecuteTransformation(w *Worker, t types.Transformation, data []string) ([]string, error) {
	_, exists := FuncRegistry[t.FuncName]
	log.Printf("Worker %d executing transformation %s of type %d\n", w.ID, t.FuncName, t.Type)
	if !exists {
		return nil, fmt.Errorf("transformation function '%s' not found", t.FuncName)
	}

	switch t.Type {
	case types.MapOp:
		fn := FuncRegistry[t.FuncName].(func(string) string)
		data = utils.Map(data, fn)

	case types.FilterOp:
		fn := FuncRegistry[t.FuncName].(func(string) bool)
		data = utils.Filter(data, fn)

	case types.FlatMapOp:
		fn := FuncRegistry[t.FuncName].(func(string) []string)
		data = utils.FlatMap(data, fn)

		// case types.ReduceOp: TODO
		//     fn := FuncRegistry[t.FuncName].(func(string, string) string)
		//     result := utils.Reduce(data, fn)
		//     data = []string{result}
	default:
		return nil, fmt.Errorf("unsupported transformation type %d", t.Type)
	}

	return data, nil
}

// ExecuteTask RPC method - must be exported
func (w *Worker) ExecuteTask(task types.Task, reply *types.TaskReply) error {
	log.Printf("Worker %d executing task %d\n", w.ID, len(task.Transformations))
	w.ActiveTasks++
	defer func() {
		w.ActiveTasks--
	}()

	data, ok := task.Data.([]string)
	if !ok {
		log.Printf("Worker %d: ERROR - Task data is not []string\n", w.ID)
		return fmt.Errorf("invalid task data type for task %d", task.ID)
	}

	// Apply transformations
	for _, t := range task.Transformations {
		transformedData, err := ExecuteTransformation(w, t, data)
		if err != nil {
			log.Printf("Worker %d: Error during transformation: %v\n", w.ID, err)
			return fmt.Errorf("transformation error in task %d: %w", task.ID, err)
		}
		data = transformedData
	}

	reply.Data = data
	log.Printf("completed task %d with %s results\n", task.ID, data)
	return nil
}

// GetStatus RPC method
func (w *Worker) GetStatus(args struct{}, reply *int) error {
	*reply = w.Status
	return nil
}

// StorePartition stores a partition on this worker
func (w *Worker) StorePartition(partitionID int, data []byte) error {
	w.Partition[partitionID] = []string{string(data)}
	log.Printf("Worker %d stored partition %d\n", w.ID, partitionID)
	return nil
}

func (w *Worker) RegisterPartition(partitionID int, reply *bool) error {
	// Aquí podrías implementar la lógica para registrar la partición
	w.Partition[partitionID] = []string{}
	log.Printf("Worker %d registered partition %d\n", w.ID, partitionID)
	*reply = true
	return nil
}

func SendResultToDriver(result string) {
}

// SendHeartbeat envía un heartbeat al driver
func (w *Worker) SendHeartbeat(driverAddress string) error {
	client, err := rpc.Dial("tcp", driverAddress)
	if err != nil {
		return fmt.Errorf("error connecting to driver: %v", err)
	}
	defer client.Close()

	w.LastHeartbeat = time.Now()
	
	heartbeat := types.Heartbeat{
		ID:            w.ID,
		Status:        w.Status,
		ActiveTasks:   w.ActiveTasks,
		Endpoint:      w.Endpoint,
		LastHeartbeat: w.LastHeartbeat,
	}

	var reply bool
	err = client.Call("Driver.WorkerHeartbeat", heartbeat, &reply)
	if err != nil {
		return fmt.Errorf("error sending heartbeat: %v", err)
	}

	log.Printf("Worker %d sent heartbeat to driver\n", w.ID)
	return nil
}

// StartHeartbeatLoop inicia un goroutine que envía heartbeats periódicamente
func (w *Worker) StartHeartbeatLoop(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			if err := w.SendHeartbeat(w.DriverAddress); err != nil {
				log.Printf("Worker %d: %v\n", w.ID, err)
			}
		}
	}()
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

	w.StartHeartbeatLoop(heartBeatInterval * time.Second)

	// Register RPC methods
	rpc.Register(w)
	listener, err := net.Listen("tcp", w.Endpoint)
	if err != nil {
		log.Fatal("Error starting worker RPC server:", err)
	}

	log.Printf("Worker %d now listening for tasks on %s\n", w.ID, w.Endpoint)
	rpc.Accept(listener)
}
