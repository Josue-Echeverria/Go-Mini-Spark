package driver

import (
	"Go-Mini-Spark/pkg/types"
	"log"
	"math/rand"
	"net/rpc"
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


func (d *Driver) Collect(id int, reply *[]string) error {

	log.Printf("%v", d.RDDRegistry)
	log.Printf("%d", id)
	r := d.RDDRegistry[id]

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

	// log.Printf("Creating tasks for RDD %d with %d partitions\n", r.ID, len(r.Partitions))


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

	d.RegisterJob(job)
	d.SaveJobState(job.ID, "running")
	 
	log .Printf("Starting job %d with %d tasks\n", job.ID, len(tasks))
	results := []string{}
	for _, task := range tasks {
		workerID := d.PartitionMap[task.PartitionID]
		workerEndpoint := d.Workers[workerID].Endpoint

		log.Printf("Sending task for partition %d to worker %d : %s\n", task.PartitionID, workerID, workerEndpoint)

		var reply types.TaskReply
		client, err := rpc.Dial("tcp", workerEndpoint)
		if err != nil {
			log.Fatal("Error connecting to worker:", err)
		}
		client.Call("Worker.ExecuteTask", task, &reply)

		results = append(results, reply.Data...)
	}

	d.SaveJobState(job.ID, "running")
	*reply = results
	return nil
}
