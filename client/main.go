package main

import (
	"fmt"
	"strings"
	. "Go-Mini-Spark/pkg/driver"
)

func main() {
	driver := ConnectDriver("localhost:9000") // Se conecta al Driver

	// Crear un RDD
	rddData := ReadRDDTextFile("data.txt", driver)

	// Transformaciones LÓGICAS (no ejecutan nada)
	rdd2 := rddData.Map(func(x string) string {
		return strings.ToUpper(x)
	})

	rdd3 := rdd2.Filter(func(x string) bool {
		return len(x) > 3
	})

	// Acción -> aquí sí se ejecuta en los workers
	result := rdd3.Collect()

	fmt.Println(result)
}
