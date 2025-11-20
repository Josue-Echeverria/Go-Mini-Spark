package main

import (
	"Go-Mini-Spark/pkg/rdd"
	"fmt"
	"strings"
)

func main() {
	driver := rdd.NewDriver("localhost:9000") // Se conecta al Master

	// Crear un RDD
	rddData := driver.TextFile("data.txt")

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
