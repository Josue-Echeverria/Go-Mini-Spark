package driver

import (
	"Go-Mini-Spark/pkg/types"
    "Go-Mini-Spark/pkg/utils"
	"log"
	"math/rand"
    "fmt"
	"net/rpc"
	"sync"
)

type RDD struct {
	ID              int
	Parent          *RDD
	Transformations []types.Transformation
	NumPartitions   int
	Partitions      []int // IDs de particiones
	Driver          *Driver
}

func (r *RDD) GetTasks() []types.Task {
    // construir pipeline
    pipeline := []types.Transformation{}
    curr := r
    for curr != nil {
        pipeline = append(curr.Transformations, pipeline...)
        curr = curr.Parent
    }

    // crear tasks
    tasks := []types.Task{}
    for i, partitionID := range r.Partitions {
        tasks = append(tasks, types.Task{
            ID:              i,
            PartitionID:     partitionID,
            Data:            r.Driver.Cache.Get(partitionID),
            Transformations: pipeline,
        })
    }
    return tasks
}

func (d *Driver) SendTasks(tasks []types.Task) [][]types.Row {
    var wg sync.WaitGroup
    wg.Add(len(tasks))
	results := make([][]types.Row, len(tasks))

    for i, task := range tasks {
        go func(i int, task types.Task) {
            defer wg.Done()

            workerID := d.PartitionMap[task.PartitionID]
            endpoint := d.Workers[workerID].Endpoint

            var rep types.TaskReply
            client, err := rpc.Dial("tcp", endpoint)
            if err != nil {
                log.Printf("Worker %d unreachable: %v\n", workerID, err)
                return
            }
            defer client.Close()

            err = client.Call("Worker.ExecuteTask", task, &rep)
            if err != nil {
                log.Printf("Task %d failed: %v\n", task.ID, err)
                return
            }

            results[i] = rep.Data
        }(i, task)
    }

    wg.Wait()
    return results
}

func (d *Driver) Map(id int, reply *int) error {
	r := d.RDDRegistry[id]
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

	// registramos el nuevo RDD en el Driver
	d.RegisterRDD(newRDD)

	*reply = newRDD.ID
	return nil
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

func (d *Driver) Collect(id int, reply *[]types.Row) error {
    r := d.RDDRegistry[id]

    tasks := r.GetTasks()

    // logging del Job
    jobID := rand.Intn(1000)
    job := types.Job{
        ID:     jobID,
        RDD:    r.ID,
        Tasks:  tasks,
        Status: "running",
    }
    d.RegisterJob(job)
    d.SaveJobState(job.ID, "running")

    results := d.SendTasks(tasks)

    // aplanar resultados
	flat := []types.Row{}
	for _, chunk := range results {
		flat = append(flat, chunk...)
	}
	*reply = flat

    d.SaveJobState(job.ID, "completed")
    *reply = flat
    return nil
}

func (d *Driver) Reduce(id int, reply *[]types.Row) error {
    r := d.RDDRegistry[id]

	newRDD := &RDD{
		ID:            newID(),
		Parent:        r,
		NumPartitions: r.NumPartitions,
		Partitions:    r.Partitions,
		Driver:        r.Driver,
	}

	newRDD.Transformations = append(newRDD.Transformations, types.Transformation{
		Type:     types.ReduceOp,
		FuncName: "Max",
	})

	r.Driver.RegisterRDD(newRDD)

    tasks := newRDD.GetTasks()
    
    // logging del Job
    jobID := rand.Intn(1000)
    job := types.Job{
        ID:     jobID,
        RDD:    newRDD.ID,
        Tasks:  tasks,
        Status: "running",
    }
    d.RegisterJob(job)
    d.SaveJobState(job.ID, "running")

    partialResults := d.SendTasks(tasks)
    
    log.Printf("Partial results: %v\n", partialResults)
	flat := []types.Row{}
	for _, chunk := range partialResults {
		flat = append(flat, chunk...)
	}

    fn := utils.FuncRegistry["Max"].(func(types.Row, types.Row) types.Row)
    result := utils.Reduce(flat, fn)
    log.Printf("Reduced result: %v\n", result)

    d.SaveJobState(job.ID, "completed")
    
    *reply = []types.Row{result}
    return nil
}

func (d *Driver) Join(request types.JoinRequest, reply *int) error {
    r1, exists1 := d.RDDRegistry[request.RddID1]
    r2, exists2 := d.RDDRegistry[request.RddID2]
    
    if !exists1 || !exists2 {
        return fmt.Errorf("one or both RDDs not found")
    }

    tasks1 := r1.GetTasks()
    tasks2 := r2.GetTasks()

    results1 := d.SendTasks(tasks1)
    results2 := d.SendTasks(tasks2)
    
	flat := []types.Row{}
	for _, chunk := range results1 {
		flat = append(flat, chunk...)
	}
    for _, chunk := range results2 {
        flat = append(flat, chunk...)
    }

    log.Printf("Join solicitado entre RDD %d y RDD %d\n", r1.ID, r2.ID)

    newPartitions := utils.Shuffle(flat, r1.NumPartitions)

    for partitionID, rows := range newPartitions {
        d.Cache.Put(partitionID, rows)
    }
    // ============================================================
    // TODO SHUFFLE STEP:
    // 
    // 1. Para cada RDD:
    //      - pedir a cada worker que envíe (key,row) por partición destino
    //        destino = hash(key) % numPartitions
    //      - el driver reenvía los rows al worker dueño de esa partición
    //
    // 2. Resultado: ambos RDD quedan con datos particionados por key.
    //
    // Implementar:
    //    d.shuffleByKey(r1)
    //    d.shuffleByKey(r2)
    //
    // ============================================================

    // Crear el nuevo RDD resultado del join


    // d.RegisterRDD(newRDD)

    // *reply = newRDD.ID
    return nil
}
