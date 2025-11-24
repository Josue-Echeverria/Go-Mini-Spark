package main

import (
	"flag"
	"log"
	"strconv"
	"Go-Mini-Spark/pkg/worker"
)

func main() {
	driverAddress := flag.String("driver", "localhost:9000", "Driver address (host:port)")
	workerAddress := flag.String("address", "localhost:9100", "Worker address (host:port)")
	maxTasks := flag.Int("max-tasks", 10, "Maximum number of tasks to handle")

	flag.Parse()

	// Support positional arguments
	args := flag.Args()
	if len(args) >= 1 {
		*driverAddress = args[0]
	}
	if len(args) >= 2 {
		*workerAddress = args[1]
	}
	if len(args) >= 3 {
		if val, err := strconv.Atoi(args[2]); err == nil {
			*maxTasks = val
		}
	}

	log.Printf("Starting worker with driver=%s, address=%s, max-tasks=%d\n", *driverAddress, *workerAddress, *maxTasks)

	worker := worker.NewWorker(*driverAddress, *workerAddress, *maxTasks)
	worker.Start(*driverAddress)
}
