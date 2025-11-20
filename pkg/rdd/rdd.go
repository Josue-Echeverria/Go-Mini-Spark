package rdd

import (
	"log"
	"net/rpc"
)

type Driver struct {
	MasterAddress string
	Client        *rpc.Client
}

type RDD struct {
	Driver *Driver
	Data   []string
}

func NewDriver(masterAddress string) *Driver {
	client, err := rpc.Dial("tcp", masterAddress)
	if err != nil {
		log.Fatal("Error connecting to master:", err)
	}

	return &Driver{
		MasterAddress: masterAddress,
		Client:        client,
	}
}

func (d *Driver) TextFile(filename string) *RDD {
	// Simular lectura de archivo por ahora
	data := []string{"hello world", "spark is great", "go programming", "distributed computing"}

	return &RDD{
		Driver: d,
		Data:   data,
	}
}

func (rdd *RDD) Map(fn func(string) string) *RDD {
	newData := make([]string, len(rdd.Data))
	for i, item := range rdd.Data {
		newData[i] = fn(item)
	}

	return &RDD{
		Driver: rdd.Driver,
		Data:   newData,
	}
}

func (rdd *RDD) Filter(fn func(string) bool) *RDD {
	var newData []string
	for _, item := range rdd.Data {
		if fn(item) {
			newData = append(newData, item)
		}
	}

	return &RDD{
		Driver: rdd.Driver,
		Data:   newData,
	}
}

func (rdd *RDD) Collect() []string {
	return rdd.Data
}
