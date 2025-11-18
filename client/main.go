package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"procesamiento-distribuido/pkg/types"
	"procesamiento-distribuido/pkg/utils"
)

type Client struct {
	masterAddress string
}

func NewClient(masterAddress string) *Client {
	return &Client{
		masterAddress: masterAddress,
	}
}

func (c *Client) submitJob(jobType, name string, config map[string]interface{}) (string, error) {
	submission := types.JobSubmission{
		Name:   name,
		Type:   types.JobType(jobType),
		Config: config,
	}

	jsonData, err := json.Marshal(submission)
	if err != nil {
		return "", err
	}

	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/jobs", c.masterAddress),
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var response types.APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return "", err
	}

	if !response.Success {
		return "", fmt.Errorf("job submission failed: %s", response.Error)
	}

	if data, ok := response.Data.(map[string]interface{}); ok {
		if jobID, ok := data["job_id"].(string); ok {
			return jobID, nil
		}
	}

	return "", fmt.Errorf("invalid response format")
}

func (c *Client) getJobStatus(jobID string) (*types.Job, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/api/jobs/%s", c.masterAddress, jobID))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("job not found")
	}

	var response types.APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, err
	}

	if !response.Success {
		return nil, fmt.Errorf("failed to get job status: %s", response.Error)
	}

	// Convert response data to Job struct
	jobData, err := json.Marshal(response.Data)
	if err != nil {
		return nil, err
	}

	var job types.Job
	if err := json.Unmarshal(jobData, &job); err != nil {
		return nil, err
	}

	return &job, nil
}

func (c *Client) waitForJob(jobID string) error {
	fmt.Printf("Waiting for job %s to complete...\n", jobID)

	for {
		job, err := c.getJobStatus(jobID)
		if err != nil {
			return err
		}

		fmt.Printf("Job Status: %s\n", job.Status)

		if job.Status == types.JobCompleted {
			fmt.Println("Job completed successfully!")
			c.printJobResults(job)
			return nil
		} else if job.Status == types.JobFailed {
			fmt.Println("Job failed!")
			return fmt.Errorf("job failed")
		}

		time.Sleep(2 * time.Second)
	}
}

func (c *Client) printJobResults(job *types.Job) {
	fmt.Printf("\n=== Job Results ===\n")
	fmt.Printf("Job ID: %s\n", job.ID)
	fmt.Printf("Name: %s\n", job.Name)
	fmt.Printf("Type: %s\n", job.Type)
	fmt.Printf("Status: %s\n", job.Status)
	fmt.Printf("Created: %s\n", job.CreatedAt.Format("2006-01-02 15:04:05"))
	if job.CompletedAt != nil {
		fmt.Printf("Completed: %s\n", job.CompletedAt.Format("2006-01-02 15:04:05"))
	}

	fmt.Printf("\nTasks: %d\n", len(job.Tasks))
	for i, task := range job.Tasks {
		fmt.Printf("  Task %d: %s (Status: %s)\n", i+1, task.ID, task.Status)
		if task.Result != nil {
			if resultJSON, err := json.MarshalIndent(task.Result, "    ", "  "); err == nil {
				fmt.Printf("    Result: %s\n", string(resultJSON))
			}
		}
		if task.Error != "" {
			fmt.Printf("    Error: %s\n", task.Error)
		}
	}
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  client <master_address> submit <job_type> <job_name> [config]")
	fmt.Println("  client <master_address> status <job_id>")
	fmt.Println("  client <master_address> wait <job_id>")
	fmt.Println("")
	fmt.Println("Job Types:")
	fmt.Println("  batch     - Batch processing job")
	fmt.Println("  streaming - Streaming processing job")
	fmt.Println("")
	fmt.Println("Examples:")
	fmt.Println("  client localhost:8080 submit batch \"Test Batch Job\"")
	fmt.Println("  client localhost:8080 submit batch \"Multiply Numbers\" '{\"operation\":\"multiply\",\"factor\":3,\"task_count\":5}'")
	fmt.Println("  client localhost:8080 status job_id_here")
	fmt.Println("  client localhost:8080 wait job_id_here")
}

func parseConfig(configStr string) map[string]interface{} {
	if configStr == "" {
		return map[string]interface{}{
			"task_count": 3,
			"operation":  "default",
		}
	}

	var config map[string]interface{}
	if err := json.Unmarshal([]byte(configStr), &config); err != nil {
		utils.LogError(fmt.Sprintf("Invalid config JSON: %v", err))
		return map[string]interface{}{
			"task_count": 3,
			"operation":  "default",
		}
	}

	return config
}

func main() {
	if len(os.Args) < 4 {
		printUsage()
		os.Exit(1)
	}

	masterAddress := os.Args[1]
	command := os.Args[2]
	client := NewClient(masterAddress)

	switch command {
	case "submit":
		if len(os.Args) < 5 {
			fmt.Println("Error: job_type and job_name are required for submit command")
			printUsage()
			os.Exit(1)
		}

		jobType := os.Args[3]
		jobName := os.Args[4]

		var config map[string]interface{}
		if len(os.Args) > 5 {
			config = parseConfig(os.Args[5])
		} else {
			config = parseConfig("")
		}

		fmt.Printf("Submitting %s job: %s\n", jobType, jobName)
		jobID, err := client.submitJob(jobType, jobName, config)
		if err != nil {
			log.Fatal("Failed to submit job:", err)
		}

		fmt.Printf("Job submitted successfully! Job ID: %s\n", jobID)

		// Optionally wait for completion
		fmt.Print("Wait for job completion? (y/n): ")
		var response string
		fmt.Scanln(&response)
		if response == "y" || response == "Y" {
			if err := client.waitForJob(jobID); err != nil {
				log.Fatal("Error waiting for job:", err)
			}
		}

	case "status":
		if len(os.Args) < 4 {
			fmt.Println("Error: job_id is required for status command")
			printUsage()
			os.Exit(1)
		}

		jobID := os.Args[3]
		job, err := client.getJobStatus(jobID)
		if err != nil {
			log.Fatal("Failed to get job status:", err)
		}

		client.printJobResults(job)

	case "wait":
		if len(os.Args) < 4 {
			fmt.Println("Error: job_id is required for wait command")
			printUsage()
			os.Exit(1)
		}

		jobID := os.Args[3]
		if err := client.waitForJob(jobID); err != nil {
			log.Fatal("Error waiting for job:", err)
		}

	default:
		fmt.Printf("Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}
