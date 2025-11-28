package main

import (
	. "Go-Mini-Spark/pkg/driver"
	"fmt"
	"Go-Mini-Spark/pkg/types"
)

func main() {
	connection := ConnectDriver("localhost:9000") // Se conecta al Driver

	
	var rddID int
	err := connection.Call("Driver.ReadRDDTextFile", "testing/largefile2.txt", &rddID)
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
	var result []types.Row
	err = connection.Call("Driver.Collect", rddID2, &result)
	if err != nil {
		fmt.Println("Error collecting results:", err)
		return
	}

	// err = connection.Call("Driver.ReadRDDTextFile", "testing/numbers.txt", &rddID)
	// if err != nil {
	// 	fmt.Println("Error reading RDD from text file:", err)
	// 	return
	// }
	
	// err = connection.Call("Driver.Map", rddID, &rddID2)
	// if err != nil {
	// 	fmt.Println("Error applying Map transformation:", err)
	// 	return
	// }
	
	// err = connection.Call("Driver.Collect", rddID2, &result)
	// if err != nil {
	// 	fmt.Println("Error collecting results:", err)
	// 	return
	// }

	// Print results properly
	fmt.Printf("Final result: %v\n", result)

	for i, r := range result {
		fmt.Printf("%v\n", i, r)
	}
}
