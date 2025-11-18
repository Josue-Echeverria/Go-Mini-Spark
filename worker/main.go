package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"procesamiento-distribuido/pkg/types"
	"procesamiento-distribuido/pkg/utils"
)

type Worker struct {
	id            string
	masterAddress string
	address       string
	maxTasks      int
	activeTasks   map[string]*types.Task
	mutex         sync.RWMutex
	stopChan      chan bool
}

func NewWorker(masterAddress, address string, maxTasks int) *Worker {
	return &Worker{
		id:            utils.GenerateID(),
		masterAddress: masterAddress,
		address:       address,
		maxTasks:      maxTasks,
		activeTasks:   make(map[string]*types.Task),
		stopChan:      make(chan bool),
	}
}

func (w *Worker) register() error {
	worker := types.Worker{
		Address:  w.address,
		MaxTasks: w.maxTasks,
	}

	jsonData, err := json.Marshal(worker)
	if err != nil {
		return err
	}

	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/workers/register", w.masterAddress),
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var response types.APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return err
	}

	if !response.Success {
		return fmt.Errorf("registration failed: %s", response.Error)
	}

	// Extract worker ID from response
	if data, ok := response.Data.(map[string]interface{}); ok {
		if workerID, ok := data["worker_id"].(string); ok {
			w.id = workerID
		}
	}

	utils.LogInfo(fmt.Sprintf("Worker registered with ID: %s", w.id))
	return nil
}

func (w *Worker) sendHeartbeat() error {
	w.mutex.RLock()
	activeTaskCount := len(w.activeTasks)
	w.mutex.RUnlock()

	heartbeat := types.Heartbeat{
		WorkerID:    w.id,
		Status:      "active",
		ActiveTasks: activeTaskCount,
		Metrics: map[string]interface{}{
			"memory_usage": "50%",
			"cpu_usage":    "30%",
		},
		Timestamp: time.Now(),
	}

	jsonData, err := json.Marshal(heartbeat)
	if err != nil {
		return err
	}

	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/workers/heartbeat", w.masterAddress),
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func (w *Worker) startHeartbeat() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := w.sendHeartbeat(); err != nil {
				utils.LogError(fmt.Sprintf("Failed to send heartbeat: %v", err))
			}
		case <-w.stopChan:
			return
		}
	}
}

func (w *Worker) executeTask(task *types.Task) {
	utils.LogInfo(fmt.Sprintf("Executing task: %s", task.ID))

	w.mutex.Lock()
	w.activeTasks[task.ID] = task
	task.Status = types.TaskRunning
	task.UpdatedAt = time.Now()
	w.mutex.Unlock()

	// Simulate task execution
	go func() {
		defer func() {
			w.mutex.Lock()
			delete(w.activeTasks, task.ID)
			w.mutex.Unlock()
		}()

		// Simulate processing time
		processingTime := time.Duration(2+task.Retries) * time.Second
		time.Sleep(processingTime)

		// Simulate task logic based on job type
		var result map[string]interface{}
		var err error

		if jobConfig, ok := task.Data["job_config"].(map[string]interface{}); ok {
			result, err = w.processTask(task, jobConfig)
		} else {
			result, err = w.processTask(task, map[string]interface{}{})
		}

		w.mutex.Lock()
		if err != nil {
			task.Status = types.TaskFailed
			task.Error = err.Error()
			task.Retries++
			utils.LogError(fmt.Sprintf("Task %s failed: %v", task.ID, err))
		} else {
			task.Status = types.TaskCompleted
			task.Result = result
			utils.LogInfo(fmt.Sprintf("Task %s completed successfully", task.ID))
		}
		task.UpdatedAt = time.Now()
		w.mutex.Unlock()

		// Report task completion to master (simplified)
		w.reportTaskStatus(task)
	}()
}

func (w *Worker) processTask(task *types.Task, jobConfig map[string]interface{}) (map[string]interface{}, error) {
	// Simulate different types of processing
	taskIndex, _ := task.Data["index"].(float64)

	// Simulate some processing based on job configuration
	if operation, ok := jobConfig["operation"].(string); ok {
		switch operation {
		case "multiply":
			factor := 2.0
			if f, ok := jobConfig["factor"].(float64); ok {
				factor = f
			}
			result := taskIndex * factor
			return map[string]interface{}{
				"operation": "multiply",
				"input":     taskIndex,
				"factor":    factor,
				"result":    result,
			}, nil

		case "process_data":
			// Simulate data processing
			return map[string]interface{}{
				"operation":       "process_data",
				"input":           taskIndex,
				"processed_items": int(taskIndex) * 100,
				"status":          "processed",
			}, nil

		default:
			return map[string]interface{}{
				"operation": "default",
				"input":     taskIndex,
				"result":    taskIndex * 2,
			}, nil
		}
	}

	// Default processing
	return map[string]interface{}{
		"input":  taskIndex,
		"result": taskIndex * 2,
	}, nil
}

func (w *Worker) reportTaskStatus(task *types.Task) {
	// In a real implementation, this would send the task status back to the master
	utils.LogDebug(fmt.Sprintf("Reporting task status: %s - %s", task.ID, task.Status))
}

func (w *Worker) handleTaskAssignment(rw http.ResponseWriter, r *http.Request) {
	var task types.Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		http.Error(rw, "Invalid request body", http.StatusBadRequest)
		return
	}

	w.mutex.RLock()
	currentTasks := len(w.activeTasks)
	w.mutex.RUnlock()

	if currentTasks >= w.maxTasks {
		http.Error(rw, "Worker at capacity", http.StatusServiceUnavailable)
		return
	}

	w.executeTask(&task)

	response := types.APIResponse{
		Success: true,
		Data:    map[string]string{"status": "accepted"},
	}
	rw.Header().Set("Content-Type", "application/json")
	json.NewEncoder(rw).Encode(response)
}

func (w *Worker) getStatus(rw http.ResponseWriter, r *http.Request) {
	w.mutex.RLock()
	activeTasks := make([]string, 0, len(w.activeTasks))
	for taskID := range w.activeTasks {
		activeTasks = append(activeTasks, taskID)
	}
	w.mutex.RUnlock()

	status := map[string]interface{}{
		"worker_id":    w.id,
		"active_tasks": len(activeTasks),
		"max_tasks":    w.maxTasks,
		"task_ids":     activeTasks,
		"status":       "active",
	}

	response := types.APIResponse{
		Success: true,
		Data:    status,
	}
	rw.Header().Set("Content-Type", "application/json")
	json.NewEncoder(rw).Encode(response)
}

func (w *Worker) Start(port string) {
	// Register with master
	err := utils.RetryWithBackoff(5, w.register)
	if err != nil {
		log.Fatal("Failed to register with master:", err)
	}

	// Start heartbeat
	go w.startHeartbeat()

	// Start HTTP server for task assignments
	http.HandleFunc("/api/tasks", w.handleTaskAssignment)
	http.HandleFunc("/api/status", w.getStatus)

	utils.LogInfo(fmt.Sprintf("Worker %s starting on port %s", w.id, port))
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func (w *Worker) Stop() {
	close(w.stopChan)
}

func main() {
	if len(os.Args) < 4 {
		log.Fatal("Usage: worker <master_address> <worker_address> <port>")
	}

	masterAddress := os.Args[1]
	workerAddress := os.Args[2] + ":" + os.Args[3]
	port := os.Args[3]

	worker := NewWorker(masterAddress, workerAddress, 5)
	worker.Start(port)
}
