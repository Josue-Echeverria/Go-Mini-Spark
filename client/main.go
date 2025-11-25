package main

import (
	"fmt"
	. "Go-Mini-Spark/pkg/driver"
)

func main() {
	driver := ConnectDriver("localhost:9000") // Se conecta al Driver

	// Crear un RDD
	rddData := driver.ReadRDDTextFile("data.txt")

	// Transformaciones LÓGICAS (no ejecutan nada)
	rdd2 := rddData.Map()

	rdd3 := rdd2.Filter()

	// Acción -> aquí sí se ejecuta en los workers
	result := rdd3.Collect()

	fmt.Println(result)
}
