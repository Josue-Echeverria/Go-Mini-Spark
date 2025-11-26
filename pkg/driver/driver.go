package driver

import (
	"Go-Mini-Spark/pkg/types"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

var rddCounter uint64

const WorkerTimeoutSeconds = 10

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
	StateDir        string
}

// Serializable info about the Driver
type DriverInfo struct {
	Workers      map[int]types.WorkerInfo
	PartitionMap map[int]int
	Port         string
}

type RDD struct {
	ID              int
	Parent          *RDD
	Transformations []types.Transformation
	NumPartitions   int
	Partitions      []int // IDs de particiones
	Driver          *Driver
	Data            []string
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

func (r *RDD) Map() *RDD {
	newRDD := &RDD{
		ID:            newID(),
		Parent:        r,
		NumPartitions: r.NumPartitions,
		Partitions:    r.Partitions,
		Driver:        r.Driver,
	}

	// agregamos la transformación pendiente
	newRDD.Transformations = append(newRDD.Transformations, types.Transformation{
		Type:     types.MapOp,
		FuncName: "ToUpper",
	})

	r.Driver.RegisterRDD(newRDD)

	return newRDD
}

func (r *RDD) Filter() *RDD {

	newRDD := &RDD{
		ID:            newID(),
		Parent:        r,
		NumPartitions: r.NumPartitions,
		Partitions:    r.Partitions,
		Driver:        r.Driver,
	}

	newRDD.Transformations = append(newRDD.Transformations, types.Transformation{
		Type:     types.FilterOp,
		FuncName: "IsLong",
	})

	r.Driver.RegisterRDD(newRDD)

	return newRDD
}

func NewDriver(port string) *Driver {
	return &Driver{
		Workers:       make(map[int]types.WorkerInfo),
		Jobs:          make(map[int]*types.Job),
		Tasks:         make(map[string]*types.Task),
		PartitionData: make(map[int][]string),
		PartitionMap:  make(map[int]int),
		Port:          port,
		StateDir:      "driver_state",
	}
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
		Jobs:          make(map[int]*types.Job),
		Tasks:         make(map[string]*types.Task),
		PartitionData: make(map[int][]string),
		PartitionMap:  driverInfo.PartitionMap,
		RDDRegistry:   make(map[int]*RDD),
		Client:        client,
		DriverAddress: masterAddress,
		StateDir: "driver_state",
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


// Recorre el lineage hacia atrás para obtener todas las transformaciones.
// Invierte las transformaciones para obtener el pipeline correcto.
// Genera una Task por cada partición del RDD.
// Para cada Task:
// // busca qué Worker tiene esa partición
// // envía el pipeline completo
// // recibe el resultado parcial
// Combina los resultados y los devuelve al usuario.

func (r *RDD) Collect() []string {

	// Construir el pipeline de transformaciones recorriendo el lineage
	pipeline := []types.Transformation{}

	curr := r
	for curr != nil {
		// Prepender las transformaciones de este RDD al inicio del pipeline
		pipeline = append(curr.Transformations, pipeline...)
		curr = curr.Parent
	}

	// Crear tareas para cada partición
	tasks := []types.Task{}
	i := 0
	for _, partitionID := range r.Partitions {
		task := types.Task{
			ID:              i,
			PartitionID:     partitionID,
			Data:            r.Driver.PartitionData[partitionID],
			Transformations: pipeline,
		}
		i++
		// log.Printf("data: %v", r.Driver.PartitionData[partitionID])
		tasks = append(tasks, task)
	}

	randomInt := rand.Intn(1000) // 0 <= n < 100
	job := types.Job{
		ID:     randomInt,
		RDD:    r.ID,
		Tasks:  tasks,
		Status: "running",
	}
	r.Driver.Jobs[job.ID] = &job
	r.Driver.SaveJobState(job.ID)

	results := []string{}
	for _, task := range tasks {
		log.Printf("Sending task for partition %d to worker %d : %s\n", task.PartitionID, r.Driver.PartitionMap[task.PartitionID], r.Driver.Workers[r.Driver.PartitionMap[task.PartitionID]].Endpoint)

		workerID := r.Driver.PartitionMap[task.PartitionID]
		worker := r.Driver.Workers[workerID]

		var reply types.TaskReply
		client, err := rpc.Dial("tcp", worker.Endpoint)
		if err != nil {
			log.Fatal("Error connecting to worker:", err)
		}
		client.Call("Worker.ExecuteTask", task, &reply)

		results = append(results, reply.Data...)
	}

	r.Driver.Jobs[job.ID].Status = "completed"
	r.Driver.SaveJobState(job.ID)

	return results
}

func (d *Driver) RegisterWorker(info types.WorkerInfo, reply *bool) error {
	d.Workers[info.ID] = info
	log.Printf("Registered worker %d at %s\n", info.ID, info.Endpoint)
	*reply = true
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

func (d *Driver) RegisterRDD(r *RDD) *RDD {
	d.RDDRegistry[r.ID] = r

	// If root RDD, allocate partitions
	if r.Parent == nil {
		d.allocatePartitions(r)
	}
	return r
}

// SaveJobState persiste el estado de un job a un archivo JSON
func (d *Driver) SaveJobState(jobID int) error {
	// Crear directorio si no existe
	if err := os.MkdirAll(d.StateDir, 0755); err != nil {
		return err
	}

	job, exists := d.Jobs[jobID]
	if !exists {
		return fmt.Errorf("job %d not found", jobID)
	}

	// Serializar job a JSON
	data, err := json.MarshalIndent(job, "", "  ")
	if err != nil {
		return err
	}

	// Guardar en archivo
	filePath := filepath.Join(d.StateDir, fmt.Sprintf("job_%d.json", jobID))
	return os.WriteFile(filePath, data, 0644)
}

// LoadJobState carga el estado de un job desde un archivo JSON
func (d *Driver) LoadJobState(jobID int) (*types.Job, error) {
	filePath := filepath.Join(d.StateDir, fmt.Sprintf("job_%d.json", jobID))

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var job types.Job
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, err
	}

	return &job, nil
}

// SaveAllJobStates persiste todos los jobs registrados
func (d *Driver) SaveAllJobStates() error {
	for jobID := range d.Jobs {
		if err := d.SaveJobState(jobID); err != nil {
			log.Printf("Error saving job state for %s: %v\n", jobID, err)
			return err
		}
	}
	return nil
}

// LoadAllJobStates carga todos los jobs desde archivos
func (d *Driver) LoadAllJobStates() error {
	entries, err := os.ReadDir(d.StateDir)
	if err != nil {
		// Si el directorio no existe, es normal en la primera ejecución
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			jobIDStr := strings.TrimSuffix(entry.Name(), ".json")
			jobIDStr = strings.TrimPrefix(jobIDStr, "job_")
			jobID := 0
			_, err := fmt.Sscanf(jobIDStr, "%d", &jobID)
			if err != nil {
				log.Printf("Error parsing job ID from %s: %v\n", jobIDStr, err)
				continue
			}
			job, err := d.LoadJobState(jobID)
			if err != nil {
				log.Printf("Error loading job state for %d: %v\n", jobID, err)
				continue
			}
			d.Jobs[jobID] = job
		}
	}
	return nil
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
	}
}

func (d *Driver) ReadRDDTextFile(filename string) *RDD {
	dataBytes, _ := os.ReadFile(filename)
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
	return rdd
}

// WorkerHeartbeat registra el heartbeat de un worker
func (d *Driver) WorkerHeartbeat(heartbeat types.Heartbeat, reply *bool) error {
	worker, exists := d.Workers[heartbeat.ID]
	if !exists {
		return fmt.Errorf("worker %d not found", heartbeat.ID)
	}

	worker.LastSeen = time.Now()
	d.Workers[heartbeat.ID] = worker
	log.Printf("Received heartbeat from worker %d (Active tasks: %d)\n", heartbeat.ID, heartbeat.ActiveTasks)
	*reply = true
	return nil
}

// IsWorkerAlive verifica si un worker está vivo (heartbeat recibido en los últimos 30 segundos)
func (d *Driver) IsWorkerAlive(workerID int) bool {
	worker, exists := d.Workers[workerID]
	if !exists || worker.Status == 500 {
		return false
	}

	return time.Since(worker.LastSeen) < time.Duration(WorkerTimeoutSeconds)*time.Second
}

// GetAliveWorkers retorna la lista de workers activos
func (d *Driver) GetAliveWorkers() []int {
	var alive []int
	for workerID := range d.Workers {
		if d.IsWorkerAlive(workerID) {
			alive = append(alive, workerID)
		}
	}
	return alive
}

func (d *Driver) reassignPartitions(partitionIDs []int) {
	aliveWorkers := d.GetAliveWorkers()

	if len(aliveWorkers) == 0 {
		log.Printf("ERROR: No alive workers available to reassign partitions\n")
		return
	}

	for i, partitionID := range partitionIDs {
		// Asignar a un worker vivo de forma round-robin
		newWorkerID := aliveWorkers[i%len(aliveWorkers)]
		oldWorkerID := d.PartitionMap[partitionID]

		log.Printf("Reassigning partition %d from worker %d to worker %d\n",
			partitionID, oldWorkerID, newWorkerID)

		d.PartitionMap[partitionID] = newWorkerID
	}
}

func (d *Driver) handleWorkerFailure(workerID int) {
	log.Printf("Handling failure of worker %d\n", workerID)

	// Reasignar particiones de este worker a workers activos
	partitionsToReassign := []int{}
	for partitionID, assignedWorker := range d.PartitionMap {
		if assignedWorker == workerID {
			partitionsToReassign = append(partitionsToReassign, partitionID)
		}
	}

	if len(partitionsToReassign) > 0 {
		log.Printf("Reassigning %d partitions from failed worker %d\n", len(partitionsToReassign), workerID)
		d.reassignPartitions(partitionsToReassign)
	}

	d.Workers[workerID] = types.WorkerInfo{
		ID:       workerID,
		Endpoint: d.Workers[workerID].Endpoint,
		Status:   500, // marcar como inactivo
	}
}

// checkWorkerHealth verifica la salud de todos los workers registrados
func (d *Driver) checkWorkerHealth() {
	for workerID, worker := range d.Workers {
		if worker.Status == 500 {
			continue // ya está marcado como inactivo
		}
		isAlive := d.IsWorkerAlive(workerID)

		if !isAlive {
			log.Printf("WARNING: Worker %d (%s) is NOT responding (no heartbeat in %d seconds)\n",
				workerID, worker.Endpoint, WorkerTimeoutSeconds)

			// TODO:
			// - Reasignar particiones a otros workers
			// - Reintentar tareas que estaban en este worker
			// - Alertar al usuario
			d.handleWorkerFailure(workerID)
		}
	}
}

// StartWorkerMonitoring inicia un goroutine que monitorea a los workers periódicamente
func (d *Driver) StartWorkerMonitoring() {
	go func() {
		ticker := time.NewTicker(WorkerTimeoutSeconds * time.Second) // Verificar cada 5 segundos
		defer ticker.Stop()

		for range ticker.C {
			d.checkWorkerHealth()
		}
	}()
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
