package driver

import (
	"Go-Mini-Spark/pkg/types"
	"log"
	"math/rand"
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
	Data            []string
}

type SerializableRDD struct {
    ID              int
    ParentID        *int // Use ID instead of pointer to parent
	Transformations []types.Transformation
	NumPartitions   int
	Partitions      []int // IDs de particiones
}

func (r *RDD) ToSerializable() *SerializableRDD {
    var parentID *int
    if r.Parent != nil {
        id := r.Parent.ID
        parentID = &id
    }
    
    return &SerializableRDD{
        ID:              r.ID,
        ParentID:        parentID,
    }
}

// Convert serializable RDD back to full RDD (when needed on client)
func (d *Driver) FromSerializable(r *SerializableRDD) *RDD {
    rdd := &RDD{
        ID:              r.ID,
        NumPartitions:   r.NumPartitions,
        Partitions:      r.Partitions,
        Transformations: r.Transformations,
        Driver:          d,
    }
    
    // Set parent if it exists
    if r.ParentID != nil {
        rdd.Parent = d.RDDRegistry[*r.ParentID]
    }
    
    return rdd
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

	// TODO 
	r.Driver.RegisterRDD(newRDD)

	return newRDD
}

func (d *Driver) Collect(id int, reply *[]types.Row) error {
    r := d.RDDRegistry[id]

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
            Data:            d.Cache.Get(partitionID),
            Transformations: pipeline,
        })
    }

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

    // ejecutar tareas en paralelo
	results := make([][]types.Row, len(tasks))
    var wg sync.WaitGroup
    wg.Add(len(tasks))

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
