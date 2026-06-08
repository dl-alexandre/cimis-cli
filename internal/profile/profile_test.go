package profile

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dl-alexandre/cimis-tsdb/storage"
)

func TestProfiler(t *testing.T) {
	t.Run("CPUProfile", func(t *testing.T) {
		profiler := NewProfiler()

		tmpFile := filepath.Join(os.TempDir(), "test_cpu_profile.prof")
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

		tmpFile := filepath.Join(os.TempDir(), "test_heap_profile.prof")
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

func TestProfilerErrorPathsAndAdditionalProfiles(t *testing.T) {
	profiler := NewProfiler()

	if err := profiler.StopCPUProfile(); err == nil {
		t.Fatal("expected error when stopping CPU profile before it starts")
	}

	invalidPath := filepath.Join(t.TempDir(), "missing", "profile.prof")
	if err := profiler.StartCPUProfile(invalidPath); err == nil {
		t.Fatal("expected error for CPU profile path in missing directory")
	}
	if err := profiler.WriteHeapProfile(invalidPath); err == nil {
		t.Fatal("expected error for heap profile path in missing directory")
	}
	originalWriteHeapProfile := writeHeapProfile
	t.Cleanup(func() {
		writeHeapProfile = originalWriteHeapProfile
	})
	writeHeapProfile = func(io.Writer) error {
		return errors.New("heap write failed")
	}
	if err := profiler.WriteHeapProfile(filepath.Join(t.TempDir(), "heap.prof")); err == nil {
		t.Fatal("expected heap write error")
	}
	if err := profiler.ProfileAllocs(invalidPath); err == nil {
		t.Fatal("expected error for allocs profile path in missing directory")
	}
	if err := profiler.ProfileGoroutines(invalidPath); err == nil {
		t.Fatal("expected error for goroutine profile path in missing directory")
	}
	if err := profiler.ProfileMutex(invalidPath); err == nil {
		t.Fatal("expected error for mutex profile path in missing directory")
	}

	runningProfile := NewProfiler()
	cpuPath := filepath.Join(t.TempDir(), "running.prof")
	if err := runningProfile.StartCPUProfile(cpuPath); err != nil {
		t.Fatalf("StartCPUProfile() error = %v", err)
	}
	if err := runningProfile.StartCPUProfile(filepath.Join(t.TempDir(), "second.prof")); err == nil {
		t.Fatal("expected error while CPU profile is already running")
	}
	if err := runningProfile.StopCPUProfile(); err != nil {
		t.Fatalf("StopCPUProfile() error = %v", err)
	}

	activeProfile := NewProfiler()
	activePath := filepath.Join(t.TempDir(), "active.prof")
	if err := activeProfile.StartCPUProfile(activePath); err != nil {
		t.Fatalf("StartCPUProfile() active error = %v", err)
	}
	otherProfile := NewProfiler()
	if err := otherProfile.StartCPUProfile(filepath.Join(t.TempDir(), "other.prof")); err == nil {
		t.Fatal("expected error when global CPU profiler is already active")
	}
	if err := activeProfile.StopCPUProfile(); err != nil {
		t.Fatalf("StopCPUProfile() active error = %v", err)
	}

	for name, writeProfile := range map[string]func(string) error{
		"allocs":     profiler.ProfileAllocs,
		"goroutines": profiler.ProfileGoroutines,
		"mutex":      profiler.ProfileMutex,
	} {
		t.Run(name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), name+".prof")
			if err := writeProfile(file); err != nil {
				t.Fatalf("%s profile error = %v", name, err)
			}
			info, err := os.Stat(file)
			if err != nil {
				t.Fatalf("profile file stat error = %v", err)
			}
			if info.Size() == 0 {
				t.Fatalf("%s profile file is empty", name)
			}
		})
	}
}

func TestMemoryStats(t *testing.T) {
	t.Run("PrintMemStats", func(t *testing.T) {
		// This should not panic
		PrintMemStats()
		t.Log("Memory stats printed successfully")
	})

	t.Run("PrintRuntimeStats", func(t *testing.T) {
		var buf bytes.Buffer
		PrintRuntimeStats(&buf)
		output := buf.String()
		if !strings.Contains(output, "Runtime Statistics") {
			t.Fatalf("PrintRuntimeStats output = %q", output)
		}
	})

	t.Run("GetMemoryStats", func(t *testing.T) {
		stats := GetMemoryStats()
		if stats.Alloc == 0 {
			t.Fatal("expected allocation stats to be populated")
		}
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

	t.Run("EnableBlockProfilingAndForceGC", func(t *testing.T) {
		EnableBlockProfiling(1)
		defer EnableBlockProfiling(0)
		ForceGC()
	})
}

func TestPerformanceMonitorCompressionAndReport(t *testing.T) {
	pm := NewPerformanceMonitor()

	if got := pm.GetAverageQueryTime(); got != 0 {
		t.Fatalf("empty query average = %v, want 0", got)
	}
	if got := pm.GetAverageIngestTime(); got != 0 {
		t.Fatalf("empty ingest average = %v, want 0", got)
	}
	if got := pm.GetAverageCompressionRatio(); got != 0 {
		t.Fatalf("empty compression average = %v, want 0", got)
	}

	pm.RecordQueryTime(10 * time.Millisecond)
	pm.RecordIngestTime(20 * time.Millisecond)
	pm.RecordCompression(storage.CompressionStats{Ratio: 2.0})
	pm.RecordCompression(storage.CompressionStats{Ratio: 4.0})

	if got := pm.GetAverageCompressionRatio(); got != 3.0 {
		t.Fatalf("compression average = %v, want 3.0", got)
	}

	var buf bytes.Buffer
	pm.PrintReport(&buf)
	output := buf.String()
	for _, want := range []string{"Performance Report", "Queries: 1", "Ingests: 1", "Compressions: 2"} {
		if !strings.Contains(output, want) {
			t.Fatalf("PrintReport output missing %q:\n%s", want, output)
		}
	}
}

func TestStartPProfServer(t *testing.T) {
	server := StartPProfServer("127.0.0.1:0")
	if server == nil {
		t.Fatal("StartPProfServer returned nil")
	}
	if err := server.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown error = %v", err)
	}
}

func TestStartPProfServerListenError(t *testing.T) {
	server := StartPProfServer("bad address")
	if server == nil {
		t.Fatal("StartPProfServer returned nil")
	}
	time.Sleep(20 * time.Millisecond)
	_ = server.Close()
}
