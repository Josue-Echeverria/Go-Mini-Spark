package client

import (
	"bytes"
	"encoding/json"
	"Go-Mini-Spark/pkg/types"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
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
