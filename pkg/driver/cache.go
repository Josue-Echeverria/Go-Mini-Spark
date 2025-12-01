package driver

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
    "Go-Mini-Spark/pkg/types"
)

// PartitionEntry represents a single partition that can exist in memory or on disk.
// It tracks the partition's location, size, and provides thread-safe access.
type PartitionEntry struct {
	PartitionID int
	inMem       []types.Row
	onDiskPath  string
	sizeBytes   int64
	mu          sync.RWMutex
}

// PartitionCache manages a collection of partitions with automatic memory management.
// When memory usage exceeds MaxMemory, it automatically spills partitions to disk.
// Thread-safe for concurrent read/write operations.
type PartitionCache struct {
	mu        sync.RWMutex
	entries   map[int]*PartitionEntry
	memBytes  int64 // tracked atomically
	MaxMemory int64
	dir       string
}

// NewPartitionCache creates a new partition cache with the specified directory and memory limit.
// The directory will be created if it doesn't exist.
// Returns an error if directory creation fails.
func NewPartitionCache(dir string, maxMem int64) (*PartitionCache, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache directory %s: %w", dir, err)
	}
	return &PartitionCache{
		entries:   make(map[int]*PartitionEntry),
		MaxMemory: maxMem,
		dir:       dir,
	}, nil
}

// estimateSizeSimple calculates basic string content size without encoding overhead
func estimateSizeSimple(data []types.Row) int64 {
    var totalSize int64
    for _, row := range data {
        // Count raw string content only
        if keyStr, ok := row.Key.(string); ok {
            totalSize += int64(len(keyStr))
        }
        if valueStr, ok := row.Value.(string); ok {
            totalSize += int64(len(valueStr))
        }
        // Add basic struct overhead (rough estimate)
        totalSize += 32 // estimated per-row overhead
    }
    return totalSize
}

// estimateSize calculates the approximate memory footprint of string slice data.
// Includes string content length plus estimated overhead (16 bytes per string for slice header/pointer).
func estimateSize(data []types.Row) int64 {
    // Option 1: Sample-based estimation
    if len(data) == 0 {
        return 0
    }
    
    // Encode a small sample to measure overhead
    sampleSize := min(10, len(data))
    sample := data[:sampleSize]
    
    var buffer bytes.Buffer
    encoder := gob.NewEncoder(&buffer)
    encoder.Encode(sample)
    
    // Calculate overhead ratio
    rawSize := int64(0)
    for _, row := range sample {
        if keyStr, ok := row.Key.(string); ok {
            rawSize += int64(len(keyStr))
        }
        if valueStr, ok := row.Value.(string); ok {
            rawSize += int64(len(valueStr))
        }
    }
    
    if rawSize > 0 {
        overhead := float64(buffer.Len()) / float64(rawSize)
        
        // Apply overhead to full dataset
        totalRawSize := int64(0)
        for _, row := range data {
            if keyStr, ok := row.Key.(string); ok {
                totalRawSize += int64(len(keyStr))
            }
            if valueStr, ok := row.Value.(string); ok {
                totalRawSize += int64(len(valueStr))
            }
        }
        
        return int64(float64(totalRawSize) * overhead)
    }
    
    // Fallback: Conservative estimate with 50% overhead
    return estimateSizeSimple(data) * 3 / 2
}


// Put stores partition data in memory and triggers spilling if memory threshold is exceeded.
// If the partition already exists, its data is replaced and memory accounting is updated.
// Spilling is triggered asynchronously to avoid blocking the caller.
func (c *PartitionCache) Put(partitionID int, data []types.Row) {
	size := estimateSize(data)

	// Find or create partition entry under cache lock
	c.mu.Lock()
	entry, exists := c.entries[partitionID]
	if !exists {
		entry = &PartitionEntry{PartitionID: partitionID}
		c.entries[partitionID] = entry
	}
	c.mu.Unlock()

	// Update entry data under entry lock
	entry.mu.Lock()
	prevSize := entry.sizeBytes
	entry.inMem = data
	entry.sizeBytes = size
	entry.onDiskPath = "" // Clear disk path since data is now in memory
	entry.mu.Unlock()

	// Update total memory usage atomically
	atomic.AddInt64(&c.memBytes, size-prevSize)

	// Trigger asynchronous spilling if memory threshold exceeded
	if atomic.LoadInt64(&c.memBytes) > c.MaxMemory {
		go c.spillIfNeeded()
	}
}

// Get retrieves partition data, loading from disk if necessary.
// Returns a copy of the data to prevent external modification.
// If data is on disk, it's loaded into memory and may trigger spilling of other partitions.
func (c *PartitionCache) Get(partitionID int) []types.Row {
	// Find the partition entry
	c.mu.RLock()
	entry, exists := c.entries[partitionID]
	c.mu.RUnlock()

	if !exists {
		log.Printf("Partition %d not found in cache", partitionID)
		return nil
	}

	// Check if data is already in memory
	entry.mu.RLock()
	if entry.inMem != nil {
		// Return a copy to prevent external modification
		dataCopy := make([]types.Row, len(entry.inMem))
		copy(dataCopy, entry.inMem)
		entry.mu.RUnlock()
		return dataCopy
	}

	// Data is on disk - get the path for loading
	diskPath := entry.onDiskPath
	entry.mu.RUnlock()

	if diskPath == "" {
		log.Printf("Partition %d has no data in memory or on disk", partitionID)
		return nil
	}

	// Load data from disk (synchronous operation)
	data := c.loadFromDisk(diskPath)
	if data == nil {
		log.Printf("Failed to load partition %d from disk at %s", partitionID, diskPath)
		return nil
	}

	// Move loaded data back to memory and update accounting
	entry.mu.Lock()
	entry.inMem = data
	entry.sizeBytes = estimateSize(data)
	entry.onDiskPath = "" // Clear disk path since data is now in memory
	entry.mu.Unlock()

	atomic.AddInt64(&c.memBytes, entry.sizeBytes)

	// Loading may have exceeded memory threshold - trigger spilling
	if atomic.LoadInt64(&c.memBytes) > c.MaxMemory {
		go c.spillIfNeeded()
	}

	return data
}

// loadFromDisk reads and deserializes partition data from a gob-encoded file.
// Returns nil if the file cannot be read or decoded.
func (c *PartitionCache) loadFromDisk(path string) []types.Row {
	// Read the entire file into memory
	fileBytes, err := os.ReadFile(path)
	if err != nil {
		log.Printf("Error reading partition file %s: %v", path, err)
		return nil
	}

	// Decode gob-encoded data
	buffer := bytes.NewBuffer(fileBytes)
	decoder := gob.NewDecoder(buffer)
	var data []types.Row

	if err := decoder.Decode(&data); err != nil {
		log.Printf("Error decoding partition data from %s: %v", path, err)
		return nil
	}

	return data
}

// spillPartition moves a partition's data from memory to disk storage.
// The data is gob-encoded and written to a file, then memory is released.
// Returns nil if the partition doesn't exist or is already on disk.
func (c *PartitionCache) spillPartition(partitionID int) error {
	// Find the partition entry
	c.mu.RLock()
	entry, exists := c.entries[partitionID]
	c.mu.RUnlock()

	if !exists {
		return nil // Partition doesn't exist - nothing to spill
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	// Check if already spilled or empty
	if entry.inMem == nil {
		return nil // Already on disk or no data
	}

	// Prepare file path and serialize data
	filePath := filepath.Join(c.dir, fmt.Sprintf("partition_%d.gob", partitionID))
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	if err := encoder.Encode(entry.inMem); err != nil {
		return fmt.Errorf("failed to encode partition %d: %w", partitionID, err)
	}

	// Write serialized data to disk
	if err := os.WriteFile(filePath, buffer.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to write partition %d to disk: %w", partitionID, err)
	}

	// Update entry state and release memory
	releasedBytes := entry.sizeBytes
	entry.inMem = nil
	entry.sizeBytes = 0
	entry.onDiskPath = filePath

	// Update global memory counter
	atomic.AddInt64(&c.memBytes, -releasedBytes)
	log.Printf("Spilled partition %d to disk (%d bytes freed)", partitionID, releasedBytes)

	return nil
}

// spillIfNeeded implements a simple eviction policy: spill largest partitions first.
// Continues until memory usage drops below MaxMemory or no more partitions can be spilled.
// This method runs in a loop to handle cases where multiple partitions need to be evicted.
func (c *PartitionCache) spillIfNeeded() {
	for atomic.LoadInt64(&c.memBytes) > c.MaxMemory {
		// Collect all partition entries under read lock
		c.mu.RLock()
		entries := make([]*PartitionEntry, 0, len(c.entries))
		for _, entry := range c.entries {
			entries = append(entries, entry)
		}
		c.mu.RUnlock()

		// Sort partitions by size (largest first) for efficient memory release
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].sizeBytes > entries[j].sizeBytes
		})

		// Attempt to spill the largest in-memory partition
		spilledAny := false
		for _, entry := range entries {
			// Quick check if partition is in memory (avoid unnecessary work)
			entry.mu.RLock()
			inMemory := entry.inMem != nil
			entry.mu.RUnlock()

			if !inMemory {
				continue // Skip partitions already on disk
			}

			// Attempt to spill this partition
			if err := c.spillPartition(entry.PartitionID); err == nil {
				spilledAny = true
				break // Success - check memory usage again
			} else {
				log.Printf("Failed to spill partition %d: %v", entry.PartitionID, err)
			}
		}

		if !spilledAny {
			// All partitions are already on disk - cannot reduce memory further
			log.Printf("Warning: Cannot reduce memory usage - all partitions already spilled")
			break
		}

		// Brief pause to prevent tight spinning and allow other operations
		time.Sleep(10 * time.Millisecond)
	}
}
