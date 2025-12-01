package worker

import (
	"Go-Mini-Spark/pkg/types"
	"Go-Mini-Spark/pkg/utils"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"time"
)

const heartBeatInterval = 2

type Worker struct {
	ID            int
	Partition     map[int][]types.Row
	TaskQueue     []types.Task
	Status        int
	Endpoint      string
	DriverAddress string
	LastHeartbeat time.Time
	ActiveTasks   int
}

func NewWorker(driverAddress, address string, maxTasks int) *Worker {
	randomInt := rand.Intn(1000) // 0 <= n < 100

	return &Worker{
		ID:            randomInt,
		Partition:     make(map[int][]types.Row),
		TaskQueue:     make([]types.Task, 0),
		Status:        0,
		Endpoint:      address,
		DriverAddress: driverAddress,
		LastHeartbeat: time.Now(),
		ActiveTasks:   0,
	}
}

func ExecuteTransformation(w *Worker, t types.Transformation, data []types.Row) ([]types.Row, error) {
	_, exists := utils.FuncRegistry[t.FuncName]
	if !exists {
		return nil, fmt.Errorf("transformation function '%s' not found", t.FuncName)
	}

	log.Printf("Worker %d executing transformation %s of type %d\n", w.ID, t.FuncName, t.Type)
	switch t.Type {
	case types.MapOp:
		fn := utils.FuncRegistry[t.FuncName].(func(types.Row) types.Row)
		data = utils.Map(data, fn)

	case types.FilterOp:
		fn := utils.FuncRegistry[t.FuncName].(func(types.Row) bool)
		data = utils.Filter(data, fn)

	case types.FlatMapOp:
		fn := utils.FuncRegistry[t.FuncName].(func(types.Row) []types.Row)
		data = utils.FlatMap(data, fn)

	case types.ReduceOp: 
		fn := utils.FuncRegistry[t.FuncName].(func(types.Row, types.Row) types.Row)
		result := utils.Reduce(data, fn)
		data = []types.Row{result}

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

	data := task.Data

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
	// log.Printf("completed task %d with %s results\n", task.ID, data)
	return nil
}

// GetStatus RPC method
func (w *Worker) GetStatus(args struct{}, reply *int) error {
	*reply = w.Status
	return nil
}

// StorePartition stores a partition on this worker
func (w *Worker) StorePartition(partitionID int, rows []types.Row) error {
    w.Partition[partitionID] = rows
    log.Printf("Worker %d stored partition %d with %d rows\n", w.ID, partitionID, len(rows))
    return nil
}

func (w *Worker) RegisterPartition(partitionID int, reply *bool) error {
	// Aquí podrías implementar la lógica para registrar la partición
	w.Partition[partitionID] = []types.Row{}
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
