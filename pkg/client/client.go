package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"Go-Mini-Spark/pkg/types"
)

// Client holds the HTTP client and base URL
type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

// NewClient creates a new API client
func NewClient(baseURL string) Client {
	return Client{
		BaseURL:    strings.TrimSuffix(baseURL, "/"),
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// SubmitJob submits a batch job
func (c *Client) SubmitJob(req types.JobRequest) (*types.JobResponse, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("error marshaling job request: %w", err)
	}

	resp, err := c.HTTPClient.Post(
		c.BaseURL+"/api/v1/jobs",
		"application/json",
		bytes.NewBuffer(data),
	)
	if err != nil {
		return nil, fmt.Errorf("error submitting job: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
	}

	var jobResp types.JobResponse
	if err := json.NewDecoder(resp.Body).Decode(&jobResp); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return &jobResp, nil
}

// GetJobStatus retrieves job status
func (c *Client) GetJobStatus(jobID string) (*types.JobResponse, error) {
	resp, err := c.HTTPClient.Get(c.BaseURL + "/api/v1/jobs/" + jobID)
	if err != nil {
		return nil, fmt.Errorf("error getting job status: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
	}

	var jobResp types.JobResponse
	if err := json.NewDecoder(resp.Body).Decode(&jobResp); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return &jobResp, nil
}

// GetJobResults retrieves job results
func (c *Client) GetJobResults(jobID string) (*types.ResultsResponse, error) {
	resp, err := c.HTTPClient.Get(c.BaseURL + "/api/v1/jobs/" + jobID + "/results")
	if err != nil {
		return nil, fmt.Errorf("error getting job results: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
	}

	var results types.ResultsResponse
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return &results, nil
}

// ServeHTTP starts the client API server
func (c *Client) ServeHTTP(port string) error {
	http.HandleFunc("/api/v1/jobs", c.handleJobs)
	http.HandleFunc("/health", c.handleHealth)
	http.HandleFunc("/", c.handleRoot)

	addr := ":" + port
	log.Printf("Client API server starting on %s", addr)
	log.Printf("Master URL: %s", c.BaseURL)
	log.Printf("\nAvailable endpoints:")
	log.Printf("  POST   /api/v1/jobs           - Submit job")
	log.Printf("  GET    /api/v1/jobs/{id}      - Get job status")
	log.Printf("  GET    /api/v1/jobs/{id}/results - Get job results")
	log.Printf("  GET    /health                - Health check")

	return http.ListenAndServe(addr, nil)
}

func (c *Client) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

func (c *Client) handleRoot(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	response := map[string]interface{}{
		"service": "Go-Mini-Spark Client API",
		"version": "1.0.0",
		"endpoints": []string{
			"POST /api/v1/jobs",
			"GET /api/v1/jobs/{id}",
			"GET /api/v1/jobs/{id}/results",
			"GET /health",
		},
	}
	json.NewEncoder(w).Encode(response)
}

func (c *Client) handleJobs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var jobReq types.JobRequest
	if err := json.NewDecoder(r.Body).Decode(&jobReq); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	log.Printf("Submitting job '%s' to master", jobReq.Name)
	jobResp, err := c.SubmitJob(jobReq)
	if err != nil {
		log.Printf("Error submitting job: %v", err)
		http.Error(w, fmt.Sprintf("Error submitting job: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(jobResp)
}
