// Package profile provides profiling and performance monitoring for the CIMIS database.
package profile

import (
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/dl-alexandre/cimis-tsdb/storage"
)

// Profiler manages CPU and memory profiling.
type Profiler struct {
	cpuFile   *os.File
	memFile   *os.File
	startTime time.Time
	mu        sync.Mutex
	isRunning bool
}

// NewProfiler creates a new profiler instance.
func NewProfiler() *Profiler {
	return &Profiler{}
}

// StartCPUProfile begins CPU profiling and writes to the specified file.
func (p *Profiler) StartCPUProfile(filename string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isRunning {
		return fmt.Errorf("profiling already running")
	}

	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("could not create CPU profile: %w", err)
	}

	if err := pprof.StartCPUProfile(f); err != nil {
		f.Close()
		return fmt.Errorf("could not start CPU profile: %w", err)
	}

	p.cpuFile = f
	p.startTime = time.Now()
	p.isRunning = true

	return nil
}

// StopCPUProfile stops CPU profiling and closes the file.
func (p *Profiler) StopCPUProfile() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.isRunning {
		return fmt.Errorf("profiling not running")
	}

	pprof.StopCPUProfile()
	if p.cpuFile != nil {
		p.cpuFile.Close()
	}

	elapsed := time.Since(p.startTime)
	fmt.Printf("CPU profile complete: %v duration\n", elapsed)

	p.isRunning = false
	return nil
}

// WriteHeapProfile writes the current heap profile to the specified file.
func (p *Profiler) WriteHeapProfile(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("could not create heap profile: %w", err)
	}
	defer f.Close()

	runtime.GC() // Get up-to-date statistics
	if err := pprof.WriteHeapProfile(f); err != nil {
		return fmt.Errorf("could not write heap profile: %w", err)
	}

	return nil
}

// ProfileAllocs writes the allocation profile to the specified file.
func (p *Profiler) ProfileAllocs(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("could not create allocs profile: %w", err)
	}
	defer f.Close()

	return pprof.Lookup("allocs").WriteTo(f, 0)
}

// ProfileGoroutines writes the goroutine profile to the specified file.
func (p *Profiler) ProfileGoroutines(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("could not create goroutine profile: %w", err)
	}
	defer f.Close()

	return pprof.Lookup("goroutine").WriteTo(f, 0)
}

// ProfileMutex writes the mutex profile to the specified file.
func (p *Profiler) ProfileMutex(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("could not create mutex profile: %w", err)
	}
	defer f.Close()

	return pprof.Lookup("mutex").WriteTo(f, 0)
}

// StartPProfServer starts an HTTP server for pprof endpoints.
func StartPProfServer(addr string) *http.Server {
	mux := http.NewServeMux()

	// pprof endpoints are registered via _ "net/http/pprof" import
	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		fmt.Printf("Starting pprof server on %s\n", addr)
		fmt.Printf("  CPU profile: curl http://%s/debug/pprof/profile\n", addr)
		fmt.Printf("  Heap: curl http://%s/debug/pprof/heap\n", addr)
		fmt.Printf("  Goroutines: curl http://%s/debug/pprof/goroutine\n", addr)
		fmt.Printf("  Allocs: curl http://%s/debug/pprof/allocs\n", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("pprof server error: %v\n", err)
		}
	}()

	return server
}

// RuntimeStats holds current runtime statistics.
type RuntimeStats struct {
	Timestamp    time.Time
	NumGoroutine int
	NumCPU       int
	MemoryAlloc  uint64
	MemoryTotal  uint64
	MemorySys    uint64
	MemoryNumGC  uint32
	HeapAlloc    uint64
	HeapSys      uint64
	HeapIdle     uint64
	HeapInuse    uint64
	HeapReleased uint64
	HeapObjects  uint64
	GCTime       time.Duration
	GCFraction   float64
}

// GetRuntimeStats returns current runtime statistics.
func GetRuntimeStats() RuntimeStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return RuntimeStats{
		Timestamp:    time.Now(),
		NumGoroutine: runtime.NumGoroutine(),
		NumCPU:       runtime.NumCPU(),
		MemoryAlloc:  m.Alloc,
		MemoryTotal:  m.TotalAlloc,
		MemorySys:    m.Sys,
		MemoryNumGC:  m.NumGC,
		HeapAlloc:    m.HeapAlloc,
		HeapSys:      m.HeapSys,
		HeapIdle:     m.HeapIdle,
		HeapInuse:    m.HeapInuse,
		HeapReleased: m.HeapReleased,
		HeapObjects:  m.HeapObjects,
		GCTime:       time.Duration(m.PauseNs[(m.NumGC+255)%256]),
		GCFraction:   float64(m.PauseTotalNs) / float64(time.Since(time.Unix(0, 0)).Nanoseconds()),
	}
}

// PrintRuntimeStats prints runtime statistics to the provided writer.
func PrintRuntimeStats(w io.Writer) {
	stats := GetRuntimeStats()

	fmt.Fprintf(w, "\n=== Runtime Statistics ===\n")
	fmt.Fprintf(w, "Timestamp: %s\n", stats.Timestamp.Format("2006-01-02 15:04:05"))
	fmt.Fprintf(w, "Goroutines: %d\n", stats.NumGoroutine)
	fmt.Fprintf(w, "CPUs: %d\n", stats.NumCPU)
	fmt.Fprintf(w, "\n--- Memory ---\n")
	fmt.Fprintf(w, "Alloc: %.2f MB\n", float64(stats.MemoryAlloc)/(1024*1024))
	fmt.Fprintf(w, "Total: %.2f MB\n", float64(stats.MemoryTotal)/(1024*1024))
	fmt.Fprintf(w, "Sys: %.2f MB\n", float64(stats.MemorySys)/(1024*1024))
	fmt.Fprintf(w, "GC Count: %d\n", stats.MemoryNumGC)
	fmt.Fprintf(w, "\n--- Heap ---\n")
	fmt.Fprintf(w, "Alloc: %.2f MB\n", float64(stats.HeapAlloc)/(1024*1024))
	fmt.Fprintf(w, "Sys: %.2f MB\n", float64(stats.HeapSys)/(1024*1024))
	fmt.Fprintf(w, "Idle: %.2f MB\n", float64(stats.HeapIdle)/(1024*1024))
	fmt.Fprintf(w, "Inuse: %.2f MB\n", float64(stats.HeapInuse)/(1024*1024))
	fmt.Fprintf(w, "Objects: %d\n", stats.HeapObjects)
}

// PerformanceMonitor tracks database performance metrics.
type PerformanceMonitor struct {
	mu           sync.RWMutex
	queryTimes   []time.Duration
	ingestTimes  []time.Duration
	compressions []storage.CompressionStats
	startTime    time.Time
}

// NewPerformanceMonitor creates a new performance monitor.
func NewPerformanceMonitor() *PerformanceMonitor {
	return &PerformanceMonitor{
		queryTimes:   make([]time.Duration, 0),
		ingestTimes:  make([]time.Duration, 0),
		compressions: make([]storage.CompressionStats, 0),
		startTime:    time.Now(),
	}
}

// RecordQueryTime records a query execution time.
func (pm *PerformanceMonitor) RecordQueryTime(d time.Duration) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.queryTimes = append(pm.queryTimes, d)
}

// RecordIngestTime records an ingest operation time.
func (pm *PerformanceMonitor) RecordIngestTime(d time.Duration) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.ingestTimes = append(pm.ingestTimes, d)
}

// RecordCompression records compression statistics.
func (pm *PerformanceMonitor) RecordCompression(stats storage.CompressionStats) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.compressions = append(pm.compressions, stats)
}

// GetAverageQueryTime returns the average query time.
func (pm *PerformanceMonitor) GetAverageQueryTime() time.Duration {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if len(pm.queryTimes) == 0 {
		return 0
	}

	var total time.Duration
	for _, t := range pm.queryTimes {
		total += t
	}
	return total / time.Duration(len(pm.queryTimes))
}

// GetAverageIngestTime returns the average ingest time.
func (pm *PerformanceMonitor) GetAverageIngestTime() time.Duration {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if len(pm.ingestTimes) == 0 {
		return 0
	}

	var total time.Duration
	for _, t := range pm.ingestTimes {
		total += t
	}
	return total / time.Duration(len(pm.ingestTimes))
}

// GetAverageCompressionRatio returns the average compression ratio.
func (pm *PerformanceMonitor) GetAverageCompressionRatio() float64 {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if len(pm.compressions) == 0 {
		return 0
	}

	var total float64
	for _, c := range pm.compressions {
		total += c.Ratio
	}
	return total / float64(len(pm.compressions))
}

// PrintReport prints a performance report.
func (pm *PerformanceMonitor) PrintReport(w io.Writer) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	fmt.Fprintf(w, "\n=== Performance Report ===\n")
	fmt.Fprintf(w, "Uptime: %v\n", time.Since(pm.startTime))
	fmt.Fprintf(w, "\n--- Operations ---\n")
	fmt.Fprintf(w, "Queries: %d (avg: %v)\n", len(pm.queryTimes), pm.GetAverageQueryTime())
	fmt.Fprintf(w, "Ingests: %d (avg: %v)\n", len(pm.ingestTimes), pm.GetAverageIngestTime())
	fmt.Fprintf(w, "Compressions: %d (avg ratio: %.2fx)\n", len(pm.compressions), pm.GetAverageCompressionRatio())
}

// EnableMutexProfiling enables mutex profiling with the specified fraction.
func EnableMutexProfiling(fraction int) {
	runtime.SetMutexProfileFraction(fraction)
}

// EnableBlockProfiling enables block profiling with the specified rate.
func EnableBlockProfiling(rate int) {
	runtime.SetBlockProfileRate(rate)
}

// ForceGC forces a garbage collection cycle.
func ForceGC() {
	runtime.GC()
}

// PrintMemStats prints detailed memory statistics.
func PrintMemStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Printf("\n=== Memory Statistics ===\n")
	fmt.Printf("Alloc = %v MB\n", bToMb(m.Alloc))
	fmt.Printf("TotalAlloc = %v MB\n", bToMb(m.TotalAlloc))
	fmt.Printf("Sys = %v MB\n", bToMb(m.Sys))
	fmt.Printf("NumGC = %v\n", m.NumGC)
	fmt.Printf("\n--- Heap ---\n")
	fmt.Printf("HeapAlloc = %v MB\n", bToMb(m.HeapAlloc))
	fmt.Printf("HeapSys = %v MB\n", bToMb(m.HeapSys))
	fmt.Printf("HeapIdle = %v MB\n", bToMb(m.HeapIdle))
	fmt.Printf("HeapInuse = %v MB\n", bToMb(m.HeapInuse))
	fmt.Printf("HeapReleased = %v MB\n", bToMb(m.HeapReleased))
	fmt.Printf("HeapObjects = %v\n", m.HeapObjects)
	fmt.Printf("\n--- Stack ---\n")
	fmt.Printf("StackInuse = %v MB\n", bToMb(m.StackInuse))
	fmt.Printf("StackSys = %v MB\n", bToMb(m.StackSys))
	fmt.Printf("\n--- GC ---\n")
	fmt.Printf("NumGC = %v\n", m.NumGC)
	fmt.Printf("PauseNs = %v ms\n", float64(m.PauseNs[(m.NumGC+255)%256])/1e6)
}

func bToMb(b uint64) float64 {
	return float64(b) / 1024 / 1024
}

// MemoryStats holds simplified memory statistics for comparison
type MemoryStats struct {
	Alloc   uint64
	NumGC   uint32
	PauseNs uint64
}

// GetMemoryStats returns current memory statistics for profiling
func GetMemoryStats() MemoryStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return MemoryStats{
		Alloc:   m.Alloc,
		NumGC:   m.NumGC,
		PauseNs: m.PauseNs[(m.NumGC+255)%256],
	}
}
