package main

import (
	"flag"
	"log"

	"Go-Mini-Spark/pkg/client"
)

func main() {
	masterURL := flag.String("url", "http://localhost:9000", "Master URL")
	port := flag.String("port", "8081", "Client API port")
	flag.Parse()

	c := client.NewClient(*masterURL)

	if err := c.ServeHTTP(*port); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
