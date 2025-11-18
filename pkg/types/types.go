package types

import (
	"time"
)

// JobType define el tipo de trabajo
type JobType string

const (
	BatchJob     JobType = "batch"
	StreamingJob JobType = "streaming"
)

// JobStatus define el estado de un trabajo
type JobStatus string

const (
	JobPending   JobStatus = "pending"
	JobRunning   JobStatus = "running"
	JobCompleted JobStatus = "completed"
	JobFailed    JobStatus = "failed"
)

// TaskStatus define el estado de una tarea
type TaskStatus string

const (
	TaskPending   TaskStatus = "pending"
	TaskRunning   TaskStatus = "running"
	TaskCompleted TaskStatus = "completed"
	TaskFailed    TaskStatus = "failed"
)

// Job representa un trabajo de procesamiento
type Job struct {
	ID          string                 `json:"id"`
	Type        JobType                `json:"type"`
	Status      JobStatus              `json:"status"`
	Name        string                 `json:"name"`
	Config      map[string]interface{} `json:"config"`
	Tasks       []Task                 `json:"tasks"`
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
	CompletedAt *time.Time             `json:"completed_at,omitempty"`
}

// Task representa una tarea individual
type Task struct {
	ID         string                 `json:"id"`
	JobID      string                 `json:"job_id"`
	WorkerID   string                 `json:"worker_id"`
	Status     TaskStatus             `json:"status"`
	Data       map[string]interface{} `json:"data"`
	Result     map[string]interface{} `json:"result,omitempty"`
	Error      string                 `json:"error,omitempty"`
	Retries    int                    `json:"retries"`
	MaxRetries int                    `json:"max_retries"`
	CreatedAt  time.Time              `json:"created_at"`
	UpdatedAt  time.Time              `json:"updated_at"`
}

// Worker representa un worker registrado
type Worker struct {
	ID            string    `json:"id"`
	Address       string    `json:"address"`
	Status        string    `json:"status"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	ActiveTasks   int       `json:"active_tasks"`
	MaxTasks      int       `json:"max_tasks"`
}

// Heartbeat representa el heartbeat de un worker
type Heartbeat struct {
	WorkerID    string                 `json:"worker_id"`
	Status      string                 `json:"status"`
	ActiveTasks int                    `json:"active_tasks"`
	Metrics     map[string]interface{} `json:"metrics"`
	Timestamp   time.Time              `json:"timestamp"`
}

// TaskAssignment representa la asignación de una tarea
type TaskAssignment struct {
	TaskID   string `json:"task_id"`
	WorkerID string `json:"worker_id"`
}

// JobSubmission representa el envío de un trabajo
type JobSubmission struct {
	Name   string                 `json:"name"`
	Type   JobType                `json:"type"`
	Config map[string]interface{} `json:"config"`
}

// APIResponse representa una respuesta de la API
type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}
