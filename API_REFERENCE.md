# Go-Mini-Spark Client API Reference

Complete API reference for the Go-Mini-Spark distributed processing system.

## Base URL

```
http://localhost:8080
```

## Authentication

Currently, no authentication is required (academic version).

## Content Type

All requests and responses use `application/json`.

## HTTP Status Codes

- `200 OK` - Success
- `201 Created` - Resource created
- `400 Bad Request` - Invalid request
- `404 Not Found` - Resource not found
- `500 Internal Server Error` - Server error

---

## Endpoints

### 1. Submit Batch Job

Submit a batch processing job with DAG definition.

**Endpoint:** `POST /api/v1/jobs`

**Request Body:**
```json
{
  "name": "string",
  "dag": {
    "nodes": [
      {
        "id": "string",
        "op": "string",
        "path": "string (optional)",
        "partitions": "integer (optional)",
        "fn": "string (optional)",
        "key": "string (optional)",
        "args": {}
      }
    ],
    "edges": [["string", "string"]]
  },
  "parallelism": "integer",
  "config": {}
}
```

**Response:** `201 Created`
```json
{
  "id": "job-abc123",
  "name": "wordcount-batch",
  "status": "ACCEPTED",
  "progress": 0.0,
  "created_at": "2024-12-02T10:00:00Z",
  "metrics": {}
}
```

**Example:**
```bash
curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d @examples/wordcount-batch.json
```

---

### 2. Submit Streaming Topology

Submit a streaming topology definition.

**Endpoint:** `POST /api/v1/topologies`

**Request Body:**
```json
{
  "name": "string",
  "operators": [
    {
      "id": "string",
      "type": "string",
      "fn": "string (optional)",
      "key": "string (optional)",
      "args": {}
    }
  ],
  "wiring": [["string", "string"]],
  "windows": {
    "type": "tumbling|sliding",
    "size_seconds": "integer",
    "slide_seconds": "integer (for sliding)"
  },
  "config": {}
}
```

**Response:** `201 Created`
```json
{
  "id": "topology-xyz789",
  "name": "log-stream-aggregation",
  "status": "ACCEPTED",
  "progress": 0.0,
  "created_at": "2024-12-02T10:00:00Z",
  "metrics": {}
}
```

**Example:**
```bash
curl -X POST http://localhost:8080/api/v1/topologies \
  -H "Content-Type: application/json" \
  -d @examples/stream-log-aggregation.json
```

---

### 3. Get Job Status

Retrieve the current status of a batch job.

**Endpoint:** `GET /api/v1/jobs/{id}`

**Parameters:**
- `id` (path) - Job ID

**Response:** `200 OK`
```json
{
  "id": "job-abc123",
  "name": "wordcount-batch",
  "status": "RUNNING|ACCEPTED|SUCCEEDED|FAILED",
  "progress": 45.5,
  "created_at": "2024-12-02T10:00:00Z",
  "completed_at": "2024-12-02T10:05:30Z",
  "metrics": {
    "tasks_completed": 18,
    "tasks_total": 40,
    "tasks_failed": 0,
    "throughput": 1500,
    "cpu_usage": 65.3,
    "memory_usage": 512000000
  },
  "error": "string (if failed)"
}
```

**Example:**
```bash
curl http://localhost:8080/api/v1/jobs/job-abc123
```

---

### 4. Get Topology Status

Retrieve the current status of a streaming topology.

**Endpoint:** `GET /api/v1/topologies/{id}`

**Parameters:**
- `id` (path) - Topology ID

**Response:** `200 OK`
```json
{
  "id": "topology-xyz789",
  "name": "log-stream-aggregation",
  "status": "RUNNING|ACCEPTED|FAILED",
  "progress": 0.0,
  "created_at": "2024-12-02T10:00:00Z",
  "metrics": {
    "events_processed": 15420,
    "throughput": 256,
    "latency_avg_ms": 12.5,
    "backpressure": false,
    "checkpoint_count": 45
  }
}
```

**Example:**
```bash
curl http://localhost:8080/api/v1/topologies/topology-xyz789
```

---

### 5. Get Job Results

Retrieve results from a completed batch job.

**Endpoint:** `GET /api/v1/jobs/{id}/results`

**Parameters:**
- `id` (path) - Job ID

**Response:** `200 OK`
```json
{
  "job_id": "job-abc123",
  "paths": [
    "/output/job-abc123/part-00000.csv",
    "/output/job-abc123/part-00001.csv",
    "/output/job-abc123/part-00002.csv"
  ],
  "format": "csv|jsonl",
  "size": 1048576
}
```

**Example:**
```bash
curl http://localhost:8080/api/v1/jobs/job-abc123/results
```

---

### 6. Ingest Events (Streaming)

Ingest events into a running streaming topology.

**Endpoint:** `POST /api/v1/ingest`

**Request Body:**
```json
{
  "events": [
    {
      "ts": "2024-12-02T10:00:00Z",
      "level": "INFO",
      "service": "api-gateway",
      "msg": "Request received"
    },
    {
      "ts": "2024-12-02T10:00:01Z",
      "level": "ERROR",
      "service": "database",
      "msg": "Connection timeout"
    }
  ]
}
```

**Response:** `200 OK`
```json
{
  "ingested": 2,
  "timestamp": "2024-12-02T10:00:02Z"
}
```

**Example:**
```bash
curl -X POST http://localhost:8080/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{"events":[{"ts":"2024-12-02T10:00:00Z","value":42}]}'
```

---

## Data Types

### Job Status

- `ACCEPTED` - Job received and queued
- `RUNNING` - Job is executing
- `SUCCEEDED` - Job completed successfully
- `FAILED` - Job failed with error

### Batch Operators

| Operator | Description | Required Fields |
|----------|-------------|-----------------|
| `read_csv` | Read CSV files | `path`, `partitions` |
| `read_jsonl` | Read JSONL files | `path`, `partitions` |
| `map` | Transform elements | `fn` |
| `filter` | Filter elements | `fn` |
| `flat_map` | Flat map transformation | `fn` |
| `reduce` | Reduce operation | `fn` |
| `reduce_by_key` | Keyed reduce | `key`, `fn` |
| `join` | Join two datasets | `key` |
| `aggregate` | Custom aggregation | `fn` |

### Streaming Operators

| Operator | Description | Required Fields |
|----------|-------------|-----------------|
| `source` | Data source | `type`, `args` |
| `map` | Transform events | `fn` |
| `filter` | Filter events | `fn` |
| `flat_map` | Flat map events | `fn` |
| `key_by` | Partition by key | `key` |
| `window_aggregate` | Window aggregation | `fn` |
| `sink` | Data sink | `type`, `args` |

### Window Types

**Tumbling Window:**
```json
{
  "type": "tumbling",
  "size_seconds": 60
}
```

**Sliding Window:**
```json
{
  "type": "sliding",
  "size_seconds": 300,
  "slide_seconds": 60
}
```

---

## Error Responses

All error responses follow this format:

```json
{
  "error": "Error message",
  "code": "ERROR_CODE",
  "details": {}
}
```

### Common Error Codes

- `INVALID_REQUEST` - Malformed request
- `JOB_NOT_FOUND` - Job ID does not exist
- `INVALID_DAG` - DAG structure is invalid
- `NO_WORKERS` - No workers available
- `EXECUTION_ERROR` - Job execution failed

**Example:**
```json
{
  "error": "Invalid DAG structure: cycle detected",
  "code": "INVALID_DAG",
  "details": {
    "cycle": ["node1", "node2", "node1"]
  }
}
```

---

## Rate Limiting

Currently no rate limiting (academic version).

## Versioning

API version is included in the URL path: `/api/v1/`

---

## Examples

### Complete Batch Workflow

```bash
# 1. Submit job
JOB_ID=$(curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d @examples/wordcount-batch.json | jq -r '.id')

# 2. Poll status
while true; do
  STATUS=$(curl http://localhost:8080/api/v1/jobs/$JOB_ID | jq -r '.status')
  echo "Status: $STATUS"
  [ "$STATUS" = "SUCCEEDED" ] && break
  sleep 2
done

# 3. Get results
curl http://localhost:8080/api/v1/jobs/$JOB_ID/results
```

### Complete Streaming Workflow

```bash
# 1. Submit topology
TOPO_ID=$(curl -X POST http://localhost:8080/api/v1/topologies \
  -H "Content-Type: application/json" \
  -d @examples/stream-log-aggregation.json | jq -r '.id')

# 2. Ingest events
curl -X POST http://localhost:8080/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d @examples/events-sample.jsonl

# 3. Check status
curl http://localhost:8080/api/v1/topologies/$TOPO_ID
```

---

## Client Library Usage

### Go Client

```go
client := NewClient("http://localhost:8080")

// Submit job
jobResp, err := client.SubmitJob(JobRequest{
    Name: "my-job",
    DAG: JobDAG{...},
    Parallelism: 4,
})

// Get status
status, err := client.GetJobStatus(jobResp.ID)

// Get results
results, err := client.GetJobResults(jobResp.ID)
```

---

## Health Check

**Endpoint:** `GET /health`

**Response:** `200 OK`
```json
{
  "status": "healthy",
  "workers": 3,
  "jobs_running": 2
}
```
