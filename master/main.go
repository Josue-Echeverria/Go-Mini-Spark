package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"procesamiento-distribuido/pkg/types"
	"procesamiento-distribuido/pkg/utils"

	"github.com/gorilla/mux"
)

type Master struct {
	workers   map[string]*types.Worker
	jobs      map[string]*types.Job
	tasks     map[string]*types.Task
	db        *sql.DB
	scheduler *utils.RoundRobinScheduler
	mutex     sync.RWMutex
	port      string
}

func NewMaster(port string) *Master {
	db, err := sql.Open("sqlite3", "./master.db")
	if err != nil {
		log.Fatal("Error opening database:", err)
	}

	master := &Master{
		workers:   make(map[string]*types.Worker),
		jobs:      make(map[string]*types.Job),
		tasks:     make(map[string]*types.Task),
		db:        db,
		scheduler: utils.NewRoundRobinScheduler(),
		port:      port,
	}

	master.initDB()
	go master.heartbeatChecker()

	return master
}

func (m *Master) initDB() {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS jobs (
			id TEXT PRIMARY KEY,
			type TEXT NOT NULL,
			status TEXT NOT NULL,
			name TEXT NOT NULL,
			config TEXT NOT NULL,
			created_at DATETIME NOT NULL,
			updated_at DATETIME NOT NULL,
			completed_at DATETIME
		)`,
		`CREATE TABLE IF NOT EXISTS tasks (
			id TEXT PRIMARY KEY,
			job_id TEXT NOT NULL,
			worker_id TEXT,
			status TEXT NOT NULL,
			data TEXT NOT NULL,
			result TEXT,
			error TEXT,
			retries INTEGER DEFAULT 0,
			max_retries INTEGER DEFAULT 3,
			created_at DATETIME NOT NULL,
			updated_at DATETIME NOT NULL,
			FOREIGN KEY(job_id) REFERENCES jobs(id)
		)`,
		`CREATE TABLE IF NOT EXISTS workers (
			id TEXT PRIMARY KEY,
			address TEXT NOT NULL,
			status TEXT NOT NULL,
			last_heartbeat DATETIME NOT NULL,
			active_tasks INTEGER DEFAULT 0,
			max_tasks INTEGER DEFAULT 10
		)`,
	}

	for _, query := range queries {
		if _, err := m.db.Exec(query); err != nil {
			log.Fatal("Error creating table:", err)
		}
	}
}

func (m *Master) registerWorker(w http.ResponseWriter, r *http.Request) {
	var worker types.Worker
	if err := json.NewDecoder(r.Body).Decode(&worker); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	m.mutex.Lock()
	worker.ID = utils.GenerateID()
	worker.Status = "active"
	worker.LastHeartbeat = time.Now()
	if worker.MaxTasks == 0 {
		worker.MaxTasks = 10
	}
	m.workers[worker.ID] = &worker
	m.mutex.Unlock()

	// Persist to database
	_, err := m.db.Exec(`
		INSERT OR REPLACE INTO workers (id, address, status, last_heartbeat, active_tasks, max_tasks)
		VALUES (?, ?, ?, ?, ?, ?)`,
		worker.ID, worker.Address, worker.Status, worker.LastHeartbeat, worker.ActiveTasks, worker.MaxTasks)

	if err != nil {
		utils.LogError(fmt.Sprintf("Error persisting worker: %v", err))
	}

	utils.LogInfo(fmt.Sprintf("Worker registered: %s at %s", worker.ID, worker.Address))

	response := types.APIResponse{
		Success: true,
		Data:    map[string]string{"worker_id": worker.ID},
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (m *Master) heartbeat(w http.ResponseWriter, r *http.Request) {
	var hb types.Heartbeat
	if err := json.NewDecoder(r.Body).Decode(&hb); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	m.mutex.Lock()
	if worker, exists := m.workers[hb.WorkerID]; exists {
		worker.LastHeartbeat = time.Now()
		worker.Status = hb.Status
		worker.ActiveTasks = hb.ActiveTasks

		// Update database
		m.db.Exec(`
			UPDATE workers SET status = ?, last_heartbeat = ?, active_tasks = ?
			WHERE id = ?`,
			worker.Status, worker.LastHeartbeat, worker.ActiveTasks, worker.ID)
	}
	m.mutex.Unlock()

	w.WriteHeader(http.StatusOK)
}

func (m *Master) submitJob(w http.ResponseWriter, r *http.Request) {
	var submission types.JobSubmission
	if err := json.NewDecoder(r.Body).Decode(&submission); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	job := &types.Job{
		ID:        utils.GenerateID(),
		Type:      submission.Type,
		Status:    types.JobPending,
		Name:      submission.Name,
		Config:    submission.Config,
		Tasks:     []types.Task{},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// Create tasks based on job type and config
	tasks := m.createTasks(job)
	job.Tasks = tasks

	m.mutex.Lock()
	m.jobs[job.ID] = job
	for _, task := range tasks {
		m.tasks[task.ID] = &task
	}
	m.mutex.Unlock()

	// Persist job
	configJSON, _ := json.Marshal(job.Config)
	_, err := m.db.Exec(`
		INSERT INTO jobs (id, type, status, name, config, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		job.ID, job.Type, job.Status, job.Name, string(configJSON), job.CreatedAt, job.UpdatedAt)

	if err != nil {
		utils.LogError(fmt.Sprintf("Error persisting job: %v", err))
	}

	// Persist tasks
	for _, task := range tasks {
		dataJSON, _ := json.Marshal(task.Data)
		m.db.Exec(`
			INSERT INTO tasks (id, job_id, status, data, retries, max_retries, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			task.ID, task.JobID, task.Status, string(dataJSON), task.Retries, task.MaxRetries, task.CreatedAt, task.UpdatedAt)
	}

	// Schedule tasks
	go m.scheduleTasks(job.ID)

	utils.LogInfo(fmt.Sprintf("Job submitted: %s (%s)", job.ID, job.Name))

	response := types.APIResponse{
		Success: true,
		Data:    map[string]string{"job_id": job.ID},
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (m *Master) createTasks(job *types.Job) []types.Task {
	tasks := []types.Task{}

	// Simple task creation based on job config
	taskCount := 1
	if count, ok := job.Config["task_count"].(float64); ok {
		taskCount = int(count)
	}

	for i := 0; i < taskCount; i++ {
		task := types.Task{
			ID:         utils.GenerateID(),
			JobID:      job.ID,
			Status:     types.TaskPending,
			Data:       map[string]interface{}{"index": i, "job_config": job.Config},
			Retries:    0,
			MaxRetries: 3,
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}
		tasks = append(tasks, task)
	}

	return tasks
}

func (m *Master) scheduleTasks(jobID string) {
	m.mutex.RLock()
	job, exists := m.jobs[jobID]
	if !exists {
		m.mutex.RUnlock()
		return
	}
	m.mutex.RUnlock()

	for _, task := range job.Tasks {
		if task.Status == types.TaskPending {
			worker := m.selectWorker()
			if worker != nil {
				m.assignTask(task.ID, worker.ID)
			}
		}
	}
}

func (m *Master) selectWorker() *types.Worker {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	availableWorkers := []*types.Worker{}
	for _, worker := range m.workers {
		if worker.Status == "active" && worker.ActiveTasks < worker.MaxTasks {
			availableWorkers = append(availableWorkers, worker)
		}
	}

	if len(availableWorkers) == 0 {
		return nil
	}

	// Simple round-robin with load awareness
	index := m.scheduler.Next(len(availableWorkers))
	return availableWorkers[index]
}

func (m *Master) assignTask(taskID, workerID string) {
	m.mutex.Lock()
	task, exists := m.tasks[taskID]
	if !exists {
		m.mutex.Unlock()
		return
	}

	task.WorkerID = workerID
	task.Status = types.TaskRunning
	task.UpdatedAt = time.Now()

	if worker, exists := m.workers[workerID]; exists {
		worker.ActiveTasks++
	}
	m.mutex.Unlock()

	// Update database
	m.db.Exec(`
		UPDATE tasks SET worker_id = ?, status = ?, updated_at = ?
		WHERE id = ?`,
		workerID, task.Status, task.UpdatedAt, taskID)

	utils.LogInfo(fmt.Sprintf("Task %s assigned to worker %s", taskID, workerID))
}

func (m *Master) getJobStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID := vars["jobId"]

	m.mutex.RLock()
	job, exists := m.jobs[jobID]
	if !exists {
		m.mutex.RUnlock()
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}
	m.mutex.RUnlock()

	response := types.APIResponse{
		Success: true,
		Data:    job,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (m *Master) heartbeatChecker() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		m.mutex.Lock()
		now := time.Now()
		for id, worker := range m.workers {
			if now.Sub(worker.LastHeartbeat) > 60*time.Second {
				utils.LogInfo(fmt.Sprintf("Worker %s marked as inactive", id))
				worker.Status = "inactive"

				// Update database
				m.db.Exec(`UPDATE workers SET status = ? WHERE id = ?`, worker.Status, id)
			}
		}
		m.mutex.Unlock()
	}
}

func (m *Master) Start() {
	router := mux.NewRouter()

	router.HandleFunc("/api/workers/register", m.registerWorker).Methods("POST")
	router.HandleFunc("/api/workers/heartbeat", m.heartbeat).Methods("POST")
	router.HandleFunc("/api/jobs", m.submitJob).Methods("POST")
	router.HandleFunc("/api/jobs/{jobId}", m.getJobStatus).Methods("GET")

	utils.LogInfo(fmt.Sprintf("Master starting on port %s", m.port))
	log.Fatal(http.ListenAndServe(":"+m.port, router))
}

func main() {
	master := NewMaster("8080")
	master.Start()
}
