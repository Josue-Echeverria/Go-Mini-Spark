package utils

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

// Logging functions
func LogInfo(message string) {
	fmt.Printf("[INFO %s] %s\n", time.Now().Format("2006-01-02 15:04:05"), message)
}

func LogError(message string) {
	fmt.Printf("[ERROR %s] %s\n", time.Now().Format("2006-01-02 15:04:05"), message)
}

func LogDebug(message string) {
	fmt.Printf("[DEBUG %s] %s\n", time.Now().Format("2006-01-02 15:04:05"), message)
}

// ID generation
func GenerateID() string {
	return uuid.New().String()
}

// Round robin scheduler
type RoundRobinScheduler struct {
	current int
}

func NewRoundRobinScheduler() *RoundRobinScheduler {
	return &RoundRobinScheduler{current: 0}
}

func (rr *RoundRobinScheduler) Next(workerCount int) int {
	if workerCount == 0 {
		return -1
	}
	index := rr.current % workerCount
	rr.current++
	return index
}

// Retry logic
func RetryWithBackoff(maxRetries int, operation func() error) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		err = operation()
		if err == nil {
			return nil
		}

		if i < maxRetries-1 {
			backoff := time.Duration(rand.Intn(1000)+500) * time.Millisecond
			LogDebug(fmt.Sprintf("Retrying in %v (attempt %d/%d)", backoff, i+1, maxRetries))
			time.Sleep(backoff)
		}
	}
	return err
}
