package main

// API Request/Response Models

// JobNode represents a node in the job DAG
type JobNode struct {
	ID         string                 `json:"id"`
	Op         string                 `json:"op"`
	Path       string                 `json:"path,omitempty"`
	Partitions int                    `json:"partitions,omitempty"`
	Fn         string                 `json:"fn,omitempty"`
	Key        string                 `json:"key,omitempty"`
	Args       map[string]interface{} `json:"args,omitempty"`
}

// JobDAG represents the DAG structure for batch jobs
type JobDAG struct {
	Nodes []JobNode  `json:"nodes"`
	Edges [][]string `json:"edges"`
}

// JobRequest represents a batch job submission request
type JobRequest struct {
	Name        string                 `json:"name"`
	DAG         JobDAG                 `json:"dag"`
	Parallelism int                    `json:"parallelism"`
	Config      map[string]interface{} `json:"config,omitempty"`
}

// TopologyOperator represents an operator in a streaming topology
type TopologyOperator struct {
	ID   string                 `json:"id"`
	Type string                 `json:"type"`
	Fn   string                 `json:"fn,omitempty"`
	Key  string                 `json:"key,omitempty"`
	Args map[string]interface{} `json:"args,omitempty"`
}

// TopologyRequest represents a streaming topology submission request
type TopologyRequest struct {
	Name      string                 `json:"name"`
	Operators []TopologyOperator     `json:"operators"`
	Wiring    [][]string             `json:"wiring"`
	Windows   map[string]interface{} `json:"windows,omitempty"`
	Config    map[string]interface{} `json:"config,omitempty"`
}

// JobResponse represents the response for job/topology operations
type JobResponse struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Status      string                 `json:"status"`
	Progress    float64                `json:"progress"`
	CreatedAt   string                 `json:"created_at"`
	CompletedAt string                 `json:"completed_at,omitempty"`
	Metrics     map[string]interface{} `json:"metrics,omitempty"`
	Error       string                 `json:"error,omitempty"`
}

// ResultsResponse represents the results of a completed job
type ResultsResponse struct {
	JobID  string   `json:"job_id"`
	Paths  []string `json:"paths"`
	Format string   `json:"format"`
	Size   int64    `json:"size"`
}

// IngestRequest represents a streaming event ingest request
type IngestRequest struct {
	Events []map[string]interface{} `json:"events"`
}
