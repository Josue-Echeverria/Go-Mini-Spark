package driver

import (
    "bytes"
    "encoding/gob"
    "fmt"
    "os"
    "path/filepath"
    "sort"
    "sync"
    "sync/atomic"
    "time"
)

type PartitionEntry struct {
    PartitionID int
    inMem       []string
    onDiskPath  string
    sizeBytes   int64
    mu          sync.RWMutex
}

type PartitionCache struct {
    mu        sync.RWMutex
    entries   map[int]*PartitionEntry
    memBytes  int64       // tracked atomically
    MaxMemory int64
    dir       string
}

// new cache
func NewPartitionCache(dir string, maxMem int64) (*PartitionCache, error) {
    if err := os.MkdirAll(dir, 0755); err != nil {
        return nil, err
    }
    return &PartitionCache{
        entries:   make(map[int]*PartitionEntry),
        MaxMemory: maxMem,
        dir:       dir,
    }, nil
}

func estimateSize(data []string) int64 {
    var s int64
    for _, str := range data {
        s += int64(len(str))
        s += 16
    }
    return s
}

// Put: guarda datos en memoria y hace spill si excede umbral
func (c *PartitionCache) Put(partitionID int, data []string) error {
    size := estimateSize(data)

    c.mu.Lock()
    e, ok := c.entries[partitionID]
    if !ok {
        e = &PartitionEntry{PartitionID: partitionID}
        c.entries[partitionID] = e
    }
    c.mu.Unlock()

    e.mu.Lock()
    // reemplaza inMem
    prev := e.sizeBytes
    e.inMem = data
    e.sizeBytes = size
    e.mu.Unlock()

    // actualizar contador total
    atomic.AddInt64(&c.memBytes, size-prev)

    // si excede umbral, spill
    if atomic.LoadInt64(&c.memBytes) > c.MaxMemory {
        go c.spillIfNeeded() // opcional: asíncrono
    }
    return nil
}

// Get: devuelve datos (carga desde disco si necesario)
func (c *PartitionCache) Get(partitionID int) ([]string, error) {
    c.mu.RLock()
    e, ok := c.entries[partitionID]
    c.mu.RUnlock()
    if !ok {
        return nil, fmt.Errorf("partition %d not found", partitionID)
    }

    e.mu.RLock()
    if e.inMem != nil {
        data := make([]string, len(e.inMem))
        copy(data, e.inMem)
        e.mu.RUnlock()
        return data, nil
    }
    onDisk := e.onDiskPath
    e.mu.RUnlock()

    if onDisk == "" {
        return nil, fmt.Errorf("no data for partition %d", partitionID)
    }

    // cargar desde disco (synch)
    data, err := c.loadFromDisk(onDisk)
    if err != nil {
        return nil, err
    }

    // colocar en memoria (y contabilizar)
    e.mu.Lock()
    e.inMem = data
    e.sizeBytes = estimateSize(data)
    e.mu.Unlock()
    atomic.AddInt64(&c.memBytes, e.sizeBytes)

    // posible spill si sobrepasa otro vez
    if atomic.LoadInt64(&c.memBytes) > c.MaxMemory {
        go c.spillIfNeeded()
    }
    return data, nil
}

func (c *PartitionCache) loadFromDisk(path string) ([]string, error) {
    b, err := os.ReadFile(path)
    if err != nil {
        return nil, err
    }
    buf := bytes.NewBuffer(b)
    dec := gob.NewDecoder(buf)
    var data []string
    if err := dec.Decode(&data); err != nil {
        return nil, err
    }
    return data, nil
}

func (c *PartitionCache) spillPartition(partitionID int) error {
    c.mu.RLock()
    e, ok := c.entries[partitionID]
    c.mu.RUnlock()
    if !ok {
        return nil
    }

    e.mu.Lock()
    if e.inMem == nil {
        e.mu.Unlock()
        return nil
    }
    data := e.inMem
    path := filepath.Join(c.dir, fmt.Sprintf("partition_%d.gob", partitionID))

    // serializar a gob
    var buf bytes.Buffer
    enc := gob.NewEncoder(&buf)
    if err := enc.Encode(data); err != nil {
        e.mu.Unlock()
        return err
    }

    if err := os.WriteFile(path, buf.Bytes(), 0644); err != nil {
        e.mu.Unlock()
        return err
    }

    // liberar memoria
    size := e.sizeBytes
    e.inMem = nil
    e.sizeBytes = 0
    e.onDiskPath = path
    e.mu.Unlock()

    atomic.AddInt64(&c.memBytes, -size)
    return nil
}

// Política simple: spill particiones más grandes hasta que memBytes <= MaxMemory
func (c *PartitionCache) spillIfNeeded() {
    for atomic.LoadInt64(&c.memBytes) > c.MaxMemory {
        // construir lista de particiones con tamaño
        c.mu.RLock()
        entries := make([]*PartitionEntry, 0, len(c.entries))
        for _, e := range c.entries {
            entries = append(entries, e)
        }
        c.mu.RUnlock()

        // ordenar por size desc
        sort.Slice(entries, func(i, j int) bool {
            return entries[i].sizeBytes > entries[j].sizeBytes
        })

        spilled := false
        for _, e := range entries {
            e.mu.RLock()
            if e.inMem == nil {
                e.mu.RUnlock()
                continue
            }
            e.mu.RUnlock()

            if err := c.spillPartition(e.PartitionID); err == nil {
                spilled = true
                break
            }
        }
        if !spilled {
            // si no se pudo spill (todos ya en disco), rompemos
            break
        }
        // pequeña pausa para evitar loop tight
        time.Sleep(10 * time.Millisecond)
    }
}
