package driver

import (
	"Go-Mini-Spark/pkg/types"
	"log"
	"net"
	"net/rpc"
	"os"
	"strings"
	"sync/atomic"
	"fmt"
)

var rddCounter uint64

const WorkerTimeoutSeconds = 10
const maxMem = 10 * 1024 * 1024 // 10 MB

type Driver struct {
	Workers         map[int]types.WorkerInfo
	Jobs            map[int]*types.Job
	Tasks           map[string]*types.Task
	PartitionMap    map[int]int
	PartitionData   map[int][]string
	RDDRegistry     map[int]*RDD
	DriverAddress   string
	Client          *rpc.Client
	Port            string
	nextPartitionID int
	Cache           *PartitionCache
	StateDir        string
}

// Serializable info about the Driver
type DriverInfo struct {
	Workers      map[int]types.WorkerInfo
	PartitionMap map[int]int
	Port         string
}


func newID() int {
	return int(atomic.AddUint64(&rddCounter, 1))
}

func keys(m map[int]types.WorkerInfo) []int {
	result := make([]int, 0, len(m))
	for k := range m {
		result = append(result, k)
	}
	return result
}

func NewDriver(port string) *Driver {
	cache, _ := NewPartitionCache("partition_cache", maxMem) // 100 MB	
	return &Driver{
		Workers:       make(map[int]types.WorkerInfo),
		Jobs:          make(map[int]*types.Job),
		RDDRegistry:   make(map[int]*RDD),
		Tasks:         make(map[string]*types.Task),
		PartitionData: make(map[int][]string),
		PartitionMap:  make(map[int]int),
		Port:          port,
		StateDir:      "driver_state",
		Cache:         cache,
	}
}

func ConnectDriver(masterAddress string) *rpc.Client {
	client, err := rpc.Dial("tcp", masterAddress)
	if err != nil {
		log.Fatal("Error connecting to master:", err)
	}

	return client
}

func (d *Driver) GetDriver(args struct{}, reply *DriverInfo) error {
	*reply = DriverInfo{
		Workers:      d.Workers,
		PartitionMap: d.PartitionMap,
		Port:         d.Port,
	}
	return nil
}

func (d *Driver) RegisterWorker(info types.WorkerInfo, reply *bool) error {
	d.Workers[info.ID] = info
	log.Printf("Registered worker %d at %s\n", info.ID, info.Endpoint)
	*reply = true
	return nil
}

func (d *Driver) GetWorkerWithPartition(partitionID int, reply *int) error {
	workerID, exists := d.PartitionMap[partitionID]
	if !exists {
		return fmt.Errorf("no worker found for partition %d", partitionID)
	}
	*reply = d.Workers[workerID].ID
	return nil
}

func (d *Driver) GetWorker(id int, reply *int) error {
	_, exists := d.Workers[id]
	if !exists {
		return fmt.Errorf("worker %d not found", id)
	}
	*reply = d.Workers[id].ID
	return nil
}

func (d *Driver) allocatePartitions(r *RDD) {
	workerIDs := keys(d.Workers)
	r.Partitions = make([]int, r.NumPartitions)

	if len(workerIDs) == 0 {
		log.Fatal("No workers available to allocate partitions")
	}

	for i := 0; i < r.NumPartitions; i++ {

		partitionID := d.nextPartitionID
		d.nextPartitionID++

		worker := workerIDs[i%len(workerIDs)]
		d.PartitionMap[partitionID] = worker
		r.Partitions[i] = partitionID

		client, err := rpc.Dial("tcp", d.Workers[worker].Endpoint)
		if err != nil {
			log.Fatal("Error connecting to worker:", err)
		}
		var reply bool
		client.Call("Worker.RegisterPartition", partitionID, &reply)
		client.Close()
	}
}

func (d *Driver) RegisterRDD(r *RDD) {

	log.Printf("Registering RDD %d with %d partitions\n", r.ID, r.NumPartitions)
	log.Printf("RDD %d has %d transformations\n", r.ID, len(r.Transformations))

	d.RDDRegistry[r.ID] = r

	// If root RDD, allocate partitions
	if r.Parent == nil {
		d.allocatePartitions(r)
	}
}

func (d *Driver) splitAndStoreData(r *RDD, lines []string) {
	chunkSize := len(lines) / r.NumPartitions

	for i := 0; i < r.NumPartitions; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if i == r.NumPartitions-1 {
			end = len(lines)
		}

		partitionData := lines[start:end]
		partitionID := r.Partitions[i]

		d.PartitionData[partitionID] = partitionData
		r.Partitions[i] = partitionID
        d.Cache.Put(partitionID, partitionData)
	}
}

func (d *Driver) ReadRDDTextFile(filename string, reply *int) error {
    dataBytes, err := os.ReadFile(filename)
    if err != nil {
        return fmt.Errorf("error reading file %s: %w", filename, err)
    }
    
    lines := strings.Split(string(dataBytes), "\n")
    rdd := &RDD{
        ID:              newID(),
        Parent:          nil,
        NumPartitions:   4,
        Transformations: []types.Transformation{},
    }

    d.RegisterRDD(rdd)
    d.splitAndStoreData(rdd, lines)

    rdd.Driver = d
    *reply = rdd.ID
    return nil
}

func (m *Driver) Start() {
	log.Printf("Driver server starting on port %s\n", m.Port)

	m.StartWorkerMonitoring()

	rpc.Register(m)
	listener, err := net.Listen("tcp", ":"+m.Port)
	if err != nil {
		log.Fatal("Error starting driver:", err)
	}

	log.Printf("Driver listening on port %s\n", m.Port)
	rpc.Accept(listener)
}
