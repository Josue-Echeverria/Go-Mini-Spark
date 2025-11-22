package main

import (
	"flag"
	"Go-Mini-Spark/pkg/driver"
)

func main() {
	port := flag.String("port", "9000", "Port for the driver to listen on")
	flag.Parse()

	d := driver.NewDriver(*port)
	d.Start()
}
