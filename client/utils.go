package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// copyFile copies a file from src to dst
func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
}

// printUsage prints the CLI usage information
func printUsage() {
	fmt.Println(`
Go-Mini-Spark Client CLI

Usage:
  client [options] <command> [args...]

Options:
  -url string
        Master URL (default "http://localhost:8080")

Commands:
  submit-job <job-file.json>
        Submit a batch job from JSON definition file

  submit-topology <topology-file.json>
        Submit a streaming topology from JSON definition file

  status <job-id>
        Get the current status of a job or topology

  results <job-id> [output-dir]
        Get the results of a completed job

  watch <job-id>
        Watch job progress in real-time

  ingest <events-file.jsonl>
        Ingest events for streaming (JSONL format)

Examples:
  # Submit a batch job
  client submit-job examples/wordcount.json

  # Check job status
  client status job-12345

  # Watch job progress
  client watch job-12345

  # Get results
  client results job-12345 ./output

  # Submit streaming topology
  client submit-topology examples/stream-agg.json

  # Ingest streaming events
  client ingest examples/events.jsonl
`)
}
