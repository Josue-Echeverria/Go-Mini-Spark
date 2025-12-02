package main

import (
	. "Go-Mini-Spark/pkg/driver"
	"Go-Mini-Spark/pkg/types"
	"fmt"
)

func main() {
	connection := ConnectDriver("localhost:9000") // Se conecta al Driver

	var rddID int
	err := connection.Call("Driver.ReadCSV", "testing/sales.csv", &rddID)
	if err != nil {
		fmt.Println("Error reading RDD from text file:", err)
		return
	}

	var rddID2 int
	err = connection.Call("Driver.ReadCSV", "testing/catalog.csv", &rddID2)
	if err != nil {
		fmt.Println("Error reading RDD from text file:", err)
		return
	}

	var joinResult int
	err = connection.Call("Driver.Join", types.JoinRequest{RddID1: rddID, RddID2: rddID2}, &joinResult)
	if err != nil {
		fmt.Println("Error performing join:", err)
		return
	}

	// err = connection.Call("Driver.Map", rddID, &rddID2)
	// if err != nil {
	// 	fmt.Println("Error mapping RDD:", err)
	// 	return
	// }

	// var result []types.Row
	// err = connection.Call("Driver.Collect", rddID2, &result)
	// if err != nil {
	// 	fmt.Println("Error collecting results:", err)
	// 	return
	// }

	// var numResult int
	// err = connection.Call("Driver.Reduce", rddID, &result)
	// if err != nil {
	// 	fmt.Println("Error reducing results:", err)
	// 	return
	// }

	// Print results properly
	// fmt.Printf("Final result: %v\n", result)

	// for i, r := range result {
	// 	fmt.Printf("%v\n", i, r)
	// }

	// fmt.Printf("Final result: %v\n", result[0].Value)
}
