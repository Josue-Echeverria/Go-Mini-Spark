package main

import (
	. "Go-Mini-Spark/pkg/driver"
	"fmt"
)

func main() {
	connection := ConnectDriver("localhost:9000") // Se conecta al Driver

	
	var rddID int
	err := connection.Call("Driver.ReadRDDTextFile", "data.txt", &rddID)
	if err != nil {
		fmt.Println("Error reading RDD from text file:", err)
		return
	}

	var rddID2 int
	err = connection.Call("Driver.Map", rddID, &rddID2)
	if err != nil {
		fmt.Println("Error applying Map transformation:", err)
		return
	}

	// rdd3 := rdd2.Filter()

	// Acción -> aquí sí se ejecuta en los workers
	var result []string
	err = connection.Call("Driver.Collect", rddID2, &result)
	if err != nil {
		fmt.Println("Error collecting results:", err)
		return
	}

	// Print results properly
	fmt.Printf("Final result: %v\n", result)

	for i, r := range result {
		fmt.Printf("Partition %d: %v\n", i, r)
	}
}
