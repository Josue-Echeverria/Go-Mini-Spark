package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// submitJobCommand handles the submit-job command
func submitJobCommand(client *Client, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: submit-job <job-definition-file.json>")
	}

	filename := args[0]
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("error reading job file: %w", err)
	}

	var jobReq JobRequest
	if err := json.Unmarshal(data, &jobReq); err != nil {
		return fmt.Errorf("error parsing job file: %w", err)
	}

	fmt.Printf("Submitting job '%s'...\n", jobReq.Name)
	jobResp, err := client.SubmitJob(jobReq)
	if err != nil {
		return err
	}

	fmt.Printf("✓ Job submitted successfully!\n")
	fmt.Printf("  Job ID: %s\n", jobResp.ID)
	fmt.Printf("  Status: %s\n", jobResp.Status)
	fmt.Printf("  Created: %s\n", jobResp.CreatedAt)
	return nil
}

// submitTopologyCommand handles the submit-topology command
func submitTopologyCommand(client *Client, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: submit-topology <topology-definition-file.json>")
	}

	filename := args[0]
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("error reading topology file: %w", err)
	}

	var topologyReq TopologyRequest
	if err := json.Unmarshal(data, &topologyReq); err != nil {
		return fmt.Errorf("error parsing topology file: %w", err)
	}

	fmt.Printf("Submitting topology '%s'...\n", topologyReq.Name)
	jobResp, err := client.SubmitTopology(topologyReq)
	if err != nil {
		return err
	}

	fmt.Printf("✓ Topology submitted successfully!\n")
	fmt.Printf("  Topology ID: %s\n", jobResp.ID)
	fmt.Printf("  Status: %s\n", jobResp.Status)
	fmt.Printf("  Created: %s\n", jobResp.CreatedAt)
	return nil
}

// statusCommand handles the status command
func statusCommand(client *Client, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: status <job-id>")
	}

	jobID := args[0]
	jobResp, err := client.GetJobStatus(jobID)
	if err != nil {
		return err
	}

	fmt.Printf("\nJob Status: %s\n", jobID)
	fmt.Printf("═══════════════════════════════════════\n")
	fmt.Printf("Name:       %s\n", jobResp.Name)
	fmt.Printf("Status:     %s\n", jobResp.Status)
	fmt.Printf("Progress:   %.1f%%\n", jobResp.Progress)
	fmt.Printf("Created:    %s\n", jobResp.CreatedAt)

	if jobResp.CompletedAt != "" {
		fmt.Printf("Completed:  %s\n", jobResp.CompletedAt)
	}

	if jobResp.Error != "" {
		fmt.Printf("Error:      %s\n", jobResp.Error)
	}

	if len(jobResp.Metrics) > 0 {
		fmt.Printf("\nMetrics:\n")
		for key, value := range jobResp.Metrics {
			fmt.Printf("  %s: %v\n", key, value)
		}
	}

	return nil
}

// resultsCommand handles the results command
func resultsCommand(client *Client, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: results <job-id> [output-dir]")
	}

	jobID := args[0]
	outputDir := "."
	if len(args) > 1 {
		outputDir = args[1]
	}

	results, err := client.GetJobResults(jobID)
	if err != nil {
		return err
	}

	fmt.Printf("\nJob Results: %s\n", jobID)
	fmt.Printf("═══════════════════════════════════════\n")
	fmt.Printf("Format: %s\n", results.Format)
	fmt.Printf("Size:   %d bytes\n", results.Size)
	fmt.Printf("\nOutput files:\n")

	for _, path := range results.Paths {
		fmt.Printf("  • %s\n", path)

		// Optionally copy to output directory
		if outputDir != "." {
			destPath := filepath.Join(outputDir, filepath.Base(path))
			if err := copyFile(path, destPath); err != nil {
				fmt.Printf("    Warning: could not copy to %s: %v\n", destPath, err)
			} else {
				fmt.Printf("    → Copied to %s\n", destPath)
			}
		}
	}

	return nil
}

// watchCommand handles the watch command
func watchCommand(client *Client, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: watch <job-id>")
	}

	jobID := args[0]
	fmt.Printf("Watching job %s...\n", jobID)
	fmt.Printf("Press Ctrl+C to stop\n\n")

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	lastStatus := ""
	lastProgress := -1.0

	for range ticker.C {
		jobResp, err := client.GetJobStatus(jobID)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}

		if jobResp.Status != lastStatus || jobResp.Progress != lastProgress {
			timestamp := time.Now().Format("15:04:05")
			fmt.Printf("[%s] Status: %-12s Progress: %5.1f%%\n",
				timestamp, jobResp.Status, jobResp.Progress)

			lastStatus = jobResp.Status
			lastProgress = jobResp.Progress
		}

		if jobResp.Status == "SUCCEEDED" || jobResp.Status == "FAILED" {
			fmt.Printf("\n✓ Job finished with status: %s\n", jobResp.Status)
			if jobResp.Error != "" {
				fmt.Printf("Error: %s\n", jobResp.Error)
			}
			break
		}
	}

	return nil
}

// ingestCommand handles the ingest command
func ingestCommand(client *Client, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: ingest <events-file.jsonl>")
	}

	filename := args[0]
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("error reading events file: %w", err)
	}

	lines := strings.Split(string(data), "\n")
	events := make([]map[string]interface{}, 0)

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var event map[string]interface{}
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			fmt.Printf("Warning: skipping invalid line: %s\n", line)
			continue
		}

		events = append(events, event)
	}

	if len(events) == 0 {
		return fmt.Errorf("no valid events found in file")
	}

	fmt.Printf("Ingesting %d events...\n", len(events))
	if err := client.IngestEvents(events); err != nil {
		return err
	}

	fmt.Printf("✓ Events ingested successfully!\n")
	return nil
}
