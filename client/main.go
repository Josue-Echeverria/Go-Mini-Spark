package main

import (
	"flag"
	"fmt"
	"os"
	"Go-Mini-Spark/pkg/client"
)

func main() {
	masterURL := flag.String("url", "http://localhost:8080", "Master URL")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		os.Exit(1)
	}

	client := client.NewClient(*masterURL)
	command := args[0]
	commandArgs := args[1:]

	var err error
	switch command {
    case "submit-job":
        err = client.SubmitJobCommand(commandArgs)
    case "status":
        err = client.StatusCommand(commandArgs)
    case "results":
        err = client.ResultsCommand(commandArgs)
    case "watch":
        err = client.WatchCommand(commandArgs)
	default:
		fmt.Printf("Unknown command: %s\n", command)
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
