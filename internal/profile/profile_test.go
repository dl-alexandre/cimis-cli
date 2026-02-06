package profile

import (
	"os"
	"sync"
	"testing"
	"time"
)

func TestProfiler(t *testing.T) {
	t.Run("CPUProfile", func(t *testing.T) {
		profiler := NewProfiler()

		tmpFile := "/tmp/test_cpu_profile.prof"
		defer os.Remove(tmpFile)

		if err := profiler.StartCPUProfile(tmpFile); err != nil {
			t.Fatalf("Failed to start CPU profile: %v", err)
		}

		// Do some work
		for i := 0; i < 1000000; i++ {
			_ = i * i
		}

		time.Sleep(100 * time.Millisecond)

		if err := profiler.StopCPUProfile(); err != nil {
			t.Fatalf("Failed to stop CPU profile: %v", err)
		}

		// Verify file exists
		if _, err := os.Stat(tmpFile); os.IsNotExist(err) {
			t.Error("CPU profile file not created")
		}

		t.Log("CPU profile created successfully")
	})

	t.Run("HeapProfile", func(t *testing.T) {
		profiler := NewProfiler()

		tmpFile := "/tmp/test_heap_profile.prof"
		defer os.Remove(tmpFile)

		// Allocate some memory
		data := make([]byte, 1024*1024)
		for i := range data {
			data[i] = byte(i % 256)
		}

		if err := profiler.WriteHeapProfile(tmpFile); err != nil {
			t.Fatalf("Failed to write heap profile: %v", err)
		}

		// Verify file exists
		if _, err := os.Stat(tmpFile); os.IsNotExist(err) {
			t.Error("Heap profile file not created")
		}

		t.Log("Heap profile created successfully")
	})

	t.Run("RuntimeStats", func(t *testing.T) {
		stats := GetRuntimeStats()

		if stats.NumGoroutine < 1 {
			t.Error("Expected at least 1 goroutine")
		}

		if stats.NumCPU < 1 {
			t.Error("Expected at least 1 CPU")
		}

		t.Logf("Runtime stats: %d goroutines, %d CPUs", stats.NumGoroutine, stats.NumCPU)
		t.Logf("Memory: %.2f MB alloc, %.2f MB sys",
			float64(stats.MemoryAlloc)/(1024*1024),
			float64(stats.MemorySys)/(1024*1024))
	})

	t.Run("PerformanceMonitor", func(t *testing.T) {
		pm := NewPerformanceMonitor()

		// Record some operations
		for i := 0; i < 10; i++ {
			pm.RecordQueryTime(time.Duration(i) * time.Millisecond)
			pm.RecordIngestTime(time.Duration(i*2) * time.Millisecond)
		}

		avgQuery := pm.GetAverageQueryTime()
		avgIngest := pm.GetAverageIngestTime()

		if avgQuery == 0 {
			t.Error("Expected non-zero average query time")
		}

		if avgIngest == 0 {
			t.Error("Expected non-zero average ingest time")
		}

		t.Logf("Average query time: %v", avgQuery)
		t.Logf("Average ingest time: %v", avgIngest)
	})
}

func TestMemoryStats(t *testing.T) {
	t.Run("PrintMemStats", func(t *testing.T) {
		// This should not panic
		PrintMemStats()
		t.Log("Memory stats printed successfully")
	})
}

func TestEnableProfiling(t *testing.T) {
	t.Run("EnableMutexProfiling", func(t *testing.T) {
		// Enable mutex profiling
		EnableMutexProfiling(1)

		// Create some mutex contention
		type counter struct {
			mu    sync.Mutex
			value int
		}

		c := &counter{}

		// Multiple goroutines accessing the counter
		for i := 0; i < 100; i++ {
			go func() {
				c.mu.Lock()
				c.value++
				c.mu.Unlock()
			}()
		}

		time.Sleep(10 * time.Millisecond)

		t.Log("Mutex profiling enabled and tested")
	})
}
