package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	masterURL := flag.String("url", "http://localhost:8080", "Master URL")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		printUsage()
		os.Exit(1)
	}

	client := NewClient(*masterURL)
	command := args[0]
	commandArgs := args[1:]

	var err error
	switch command {
	case "submit-job":
		err = submitJobCommand(client, commandArgs)
	case "submit-topology":
		err = submitTopologyCommand(client, commandArgs)
	case "status":
		err = statusCommand(client, commandArgs)
	case "results":
		err = resultsCommand(client, commandArgs)
	case "watch":
		err = watchCommand(client, commandArgs)
	case "ingest":
		err = ingestCommand(client, commandArgs)
	case "help", "-h", "--help":
		printUsage()
		os.Exit(0)
	default:
		fmt.Printf("Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
