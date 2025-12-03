package driver

import (
	"Go-Mini-Spark/pkg/types"
	"fmt"
	"log"
	"time"
)

// GetAliveWorkers retorna la lista de workers activos
func (d *Driver) GetAliveWorkers() []int {
	d.WorkerMutex.Lock()
	defer d.WorkerMutex.Unlock()

	var alive []int
	for workerID := range d.Workers {
		if d.IsWorkerAlive(workerID) {
			alive = append(alive, workerID)
		}
	}
	return alive
}

// WorkerHeartbeat registra el heartbeat de un worker
func (d *Driver) WorkerHeartbeat(heartbeat types.Heartbeat, reply *bool) error {
	d.WorkerMutex.Lock()
	defer d.WorkerMutex.Unlock()

	worker, exists := d.Workers[heartbeat.ID]
	if !exists {
		d.Workers[heartbeat.ID] = types.WorkerInfo{
			ID:       heartbeat.ID,
			Endpoint: heartbeat.Endpoint,
			Status:   200,
			LastSeen: time.Now(),
		}
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

	d.WorkerMutex.Lock()
	d.Workers[workerID] = types.WorkerInfo{
		ID:       workerID,
		Endpoint: d.Workers[workerID].Endpoint,
		Status:   500, // marcar como inactivo
	}
	d.WorkerMutex.Unlock()
}

// checkWorkerHealth verifica la salud de todos los workers registrados
func (d *Driver) checkWorkerHealth() {
	d.WorkerMutex.Lock()
	workersCopy := make(map[int]types.WorkerInfo)
	for k, v := range d.Workers {
		workersCopy[k] = v
	}
	d.WorkerMutex.Unlock()

	for workerID, worker := range workersCopy {
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
