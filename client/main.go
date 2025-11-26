package main

import (
	. "Go-Mini-Spark/pkg/driver"
	"fmt"
)

func main() {
	driver := ConnectDriver("localhost:9000") // Se conecta al Driver

	// Crear un RDD
	rddData := driver.ReadRDDTextFile("data.txt")

	rdd2 := rddData.Map()

	// rdd3 := rdd2.Filter()

	// Acción -> aquí sí se ejecuta en los workers
	result := rdd2.Collect()

	// Print results properly
	fmt.Printf("Final result: %v\n", result)

	for i, r := range result {
		fmt.Printf("Partition %d: %v\n", i, r)
	}
}
