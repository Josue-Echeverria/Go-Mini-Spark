package driver

import (
	"Go-Mini-Spark/pkg/types"
	"log"
	"net"
	"net/rpc"
	"sync/atomic"
)

type TransformationType int
var rddCounter uint64
const (
    MapOp TransformationType = iota
    FilterOp
)

type Task struct {
	ID          int
	PartitionID int
	Transformations []Transformation
	ExecuteFunc func(data interface{}) interface{}
}

type Driver struct {
	Workers      map[int]types.WorkerInfo
	Jobs         map[string]*types.Job
	Tasks        map[string]*Task
	PartitionMap map[int]int
	RDDRegistry  map[int]*RDD
	DriverAddress string
	Client        *rpc.Client
	Port         string
	nextPartitionID int
}

// Serializable info about the Driver
type DriverInfo struct {
    Workers      map[int]types.WorkerInfo
    PartitionMap map[int]int
    Port         string
}

type Transformation struct {
    Type TransformationType
    Func interface{}
}

type RDD struct {
    ID           int
    Parent       *RDD
    Transformations []Transformation
    NumPartitions int
	Partitions   []int
	Driver *Driver
	Data   []string
}

func newID() int {
    return int(atomic.AddUint64(&rddCounter, 1))
}

func ConnectDriver(masterAddress string) *Driver {
	client, err := rpc.Dial("tcp", masterAddress)
	if err != nil {
		log.Fatal("Error connecting to master:", err)
	}
	
    var driverInfo DriverInfo
    err = client.Call("Driver.GetDriver", struct{}{}, &driverInfo)
	if err != nil {
		log.Fatal("Error registering with driver:", err)
	}

    driver := &Driver{
        Workers:       driverInfo.Workers,
        Jobs:          make(map[string]*types.Job),
        Tasks:         make(map[string]*Task),
        PartitionMap:  driverInfo.PartitionMap,
        RDDRegistry:   make(map[int]*RDD),
        Client:        client,
        DriverAddress: masterAddress,
        Port:          driverInfo.Port,
    }

    return driver
}

func (d *Driver) GetDriver(args struct{}, reply *DriverInfo) error {
    *reply = DriverInfo{
        Workers:      d.Workers,
        PartitionMap: d.PartitionMap,
        Port:         d.Port,
    }
    return nil
}

func (r *RDD) Map(f interface{}) *RDD {
    newRDD := &RDD{
        ID:             newID(),
        Parent:         r,
        NumPartitions:  r.NumPartitions,
		Partitions:     r.Partitions,
        Driver:         r.Driver,
    }

    // agregamos la transformación pendiente
    newRDD.Transformations = append(newRDD.Transformations, Transformation{
        Type: MapOp,
        Func: f,
    })

	r.Driver.RegisterRDD(newRDD)

    return newRDD
}

func (r *RDD) Filter(f interface{}) *RDD {
    newRDD := &RDD{
        ID:             newID(),
        Parent:         r,
        NumPartitions:  r.NumPartitions,
		Partitions:     r.Partitions,
        Driver:         r.Driver,
    }

    newRDD.Transformations = append(newRDD.Transformations, Transformation{
        Type: FilterOp,
        Func: f,
    })

	r.Driver.RegisterRDD(newRDD)

    return newRDD
}

// Recorre el lineage hacia atrás para obtener todas las transformaciones.
// Invierte las transformaciones para obtener el pipeline correcto.
// Genera una Task por cada partición del RDD.
// Para cada Task:
// // busca qué Worker tiene esa partición
// // envía el pipeline completo
// // recibe el resultado parcial
// Combina los resultados y los devuelve al usuario.

func (r *RDD) Collect() []string {
	log.Printf("Collecting data from RDD %d\n", r.ID)
	log.Printf("Applying %d transformations\n", len(r.Transformations))
	log.Printf("Data: %v\n", r.Data)

	pipeline := []Transformation{}
	curr := r
	for curr != nil {
		pipeline = append([]Transformation{}, curr.Transformations...)
		curr = curr.Parent
	}

	for i, j := 0, len(pipeline)-1; i < j; i, j = i+1, j-1 {
        pipeline[i], pipeline[j] = pipeline[j], pipeline[i]
    }

	tasks := []Task{}

	for partitionID := range r.Partitions {
		task := Task{
			PartitionID:     partitionID,
			Transformations: pipeline,  // el pipeline ya invertido
		}
	
		tasks = append(tasks, task)
	}
	
	for _, task := range tasks {
		log.Printf("Sending task for partition %d to worker %d\n", task.PartitionID, r.Driver.PartitionMap[task.PartitionID])
		// Aquí se enviaría la tarea al worker correspondiente y se recogería el resultado
	}

	return r.Data
}

func NewDriver(port string) *Driver {
	return &Driver{
		Workers:      make(map[int]types.WorkerInfo),
		Jobs:         make(map[string]*types.Job),
		Tasks:        make(map[string]*Task),
		PartitionMap: make(map[int]int),
		Port:         port,
	}
}

func runJob() {
}

func assignTask(partitionID string, workerID string) {
}

func (d *Driver) RegisterWorker(info types.WorkerInfo, reply *bool) error {
	d.Workers[info.ID] = info
	log.Printf("Registered worker %d at %s\n", info.ID, info.Endpoint)
	*reply = true
	return nil
}

func (d *Driver) allocatePartitions(r *RDD) {
    if len(d.Workers) == 0 {
        panic("No workers connected")
    }

	workerIDs := make([]int, 0, len(d.Workers))
	for id := range d.Workers {
		workerIDs = append(workerIDs, id)
	}

	r.Partitions = make([]int, r.NumPartitions)

	for i := 0; i < r.NumPartitions; i++ {
		partitionID := d.nextPartitionID
		d.nextPartitionID++

		// Assign workers round-robin
		worker := workerIDs[i%len(workerIDs)]

		d.PartitionMap[partitionID] = worker
		r.Partitions[i] = partitionID
	}
}

func (d *Driver) RegisterRDD(r *RDD) *RDD {
    d.RDDRegistry[r.ID] = r

	// If root RDD, allocate partitions
    if r.Parent == nil {
        d.allocatePartitions(r)
    }
    return r
}

func ReadRDDTextFile(filename string, driver *Driver) *RDD {
	// Simular lectura de archivo por ahora
	data := []string{"hello world", "spark is great", "go programming", "distributed computing"}
	rdd := &RDD{
		ID:             newID(),
		Parent:         nil,
		NumPartitions:  4,
		Driver:         driver,
		Data:           data,
	}
	driver.RegisterRDD(rdd)
	return rdd
}

func (m *Driver) Start() {
	log.Printf("Driver server starting on port %s\n", m.Port)

	rpc.Register(m)
	listener, err := net.Listen("tcp", ":"+m.Port)
	if err != nil {
		log.Fatal("Error starting driver:", err)
	}

	log.Printf("Driver listening on port %s\n", m.Port)
	rpc.Accept(listener)
}