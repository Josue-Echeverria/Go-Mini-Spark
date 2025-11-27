package driver

import (
	"Go-Mini-Spark/pkg/types"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

func (d *Driver) RegisterJob(job types.Job) {
	log.Printf("Registering job %d with status %s", job.ID, job.Status)
	d.Jobs[job.ID] = &job
}

// SaveJobState RPC method - persiste el estado de un job a un archivo JSON
func (d *Driver) SaveJobState(jobID int, state string) {
	log.Printf("Saving job %d with state: %s", jobID, state)

	// Crear directorio si no existe
	if err := os.MkdirAll(d.StateDir, 0755); err != nil {
		log.Printf("Error creating state directory: %v", err)
	}

	job, exists := d.Jobs[jobID]
	if !exists {
		log.Printf("Job %d not found", jobID)
	}

	if state != "" {
		job.Status = state
	}

	// Serializar job a JSON
	data, err := json.MarshalIndent(job, "", "  ")
	if err != nil {
		log.Printf("Error marshaling job %d to JSON: %v", jobID, err)
	}

	// Guardar en archivo
	filePath := filepath.Join(d.StateDir, fmt.Sprintf("job_%d.json", jobID))
	err = os.WriteFile(filePath, data, 0644)
	if err != nil {
		log.Printf("Error writing job state to file: %v", err)
	}

	log.Printf("Job %d state saved successfully", jobID)
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
