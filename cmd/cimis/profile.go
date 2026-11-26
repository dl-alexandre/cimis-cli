// Profile command for the CIMIS database CLI.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/dl-alexandre/cimis-cli/internal/api"
	"github.com/dl-alexandre/cimis-cli/internal/profile"
	"github.com/dl-alexandre/cimis-tsdb/storage"
)

type ProfileCmd struct {
	CPU        string        `help:"CPU profile output file" name:"cpu"`
	Heap       string        `help:"Heap profile output file" name:"heap"`
	Allocs     string        `help:"Allocations profile output file" name:"allocs"`
	Goroutines string        `help:"Goroutine profile output file" name:"goroutines"`
	Mutex      string        `help:"Mutex profile output file" name:"mutex"`
	Duration   time.Duration `help:"Profiling duration" name:"duration" default:"30s"`
	Server     string        `help:"Start pprof server on address (e.g., localhost:6060)" name:"server"`
	Stats      bool          `help:"Print runtime statistics" name:"stats"`
	Station    int           `help:"Station ID for memory profiling during ingest" name:"station"`
	Year       int           `help:"Year for memory profiling during ingest" name:"year"`
}

var (
	notifyProfileSignal = func(ch chan<- os.Signal) {
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	}
	stopProfileSignal     = signal.Stop
	getProfileMemoryStats = profile.GetMemoryStats
	stopCPUProfile        = func(profiler *profile.Profiler) error {
		return profiler.StopCPUProfile()
	}
)

func cmdProfile(dataDir string, args []string) {
	fatalIfErr(runProfile(dataDir, args))
}

func runProfile(dataDir string, args []string) error {
	var options ProfileCmd
	if err := parseCommandOptions("profile", args, &options); err != nil {
		return err
	}
	return runProfileOptions(dataDir, options)
}

func (c *ProfileCmd) Run(globals *Globals) error {
	return runProfileOptions(globals.DataDir, *c)
}

func runProfileOptions(dataDir string, options ProfileCmd) error {
	// Start pprof server if requested
	if options.Server != "" {
		pprofServer := profile.StartPProfServer(options.Server)
		fmt.Printf("pprof server started on %s\n", options.Server)
		fmt.Printf("  CPU profile: curl http://%s/debug/pprof/profile\n", options.Server)
		fmt.Printf("  Heap: curl http://%s/debug/pprof/heap\n", options.Server)
		fmt.Printf("  Goroutines: curl http://%s/debug/pprof/goroutine\n", options.Server)
		fmt.Printf("  Allocs: curl http://%s/debug/pprof/allocs\n", options.Server)
		fmt.Println("Press Ctrl+C to stop...")

		// Wait for interrupt
		sigChan := make(chan os.Signal, 1)
		notifyProfileSignal(sigChan)
		<-sigChan
		stopProfileSignal(sigChan)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = pprofServer.Shutdown(ctx)
		fmt.Println("\nShutting down...")
		return nil
	}

	// Print runtime stats
	if options.Stats {
		profile.PrintRuntimeStats(os.Stdout)
		return nil
	}

	profiler := profile.NewProfiler()

	// CPU profiling
	if options.CPU != "" {
		fmt.Printf("Starting CPU profile for %v...\n", options.Duration)
		if err := profiler.StartCPUProfile(options.CPU); err != nil {
			return fmt.Errorf("failed to start CPU profile: %w", err)
		}

		time.Sleep(options.Duration)

		if err := stopCPUProfile(profiler); err != nil {
			return fmt.Errorf("failed to stop CPU profile: %w", err)
		}
		fmt.Printf("CPU profile written to: %s\n", options.CPU)
	}

	// Heap profiling
	if options.Heap != "" {
		if err := profiler.WriteHeapProfile(options.Heap); err != nil {
			return fmt.Errorf("failed to write heap profile: %w", err)
		}
		fmt.Printf("Heap profile written to: %s\n", options.Heap)
	}

	// Allocs profiling
	if options.Allocs != "" {
		if err := profiler.ProfileAllocs(options.Allocs); err != nil {
			return fmt.Errorf("failed to write allocs profile: %w", err)
		}
		fmt.Printf("Allocs profile written to: %s\n", options.Allocs)
	}

	// Goroutine profiling
	if options.Goroutines != "" {
		if err := profiler.ProfileGoroutines(options.Goroutines); err != nil {
			return fmt.Errorf("failed to write goroutine profile: %w", err)
		}
		fmt.Printf("Goroutine profile written to: %s\n", options.Goroutines)
	}

	// Mutex profiling
	if options.Mutex != "" {
		profile.EnableMutexProfiling(1)
		time.Sleep(options.Duration)
		if err := profiler.ProfileMutex(options.Mutex); err != nil {
			return fmt.Errorf("failed to write mutex profile: %w", err)
		}
		fmt.Printf("Mutex profile written to: %s\n", options.Mutex)
	}

	// Memory profiling during ingestion
	if options.Station > 0 && options.Year > 0 {
		profileMemoryDuringIngest(options.Station, options.Year, dataDir)
		return nil
	}

	// If no specific profile requested, print stats
	if options.CPU == "" && options.Heap == "" && options.Allocs == "" && options.Goroutines == "" && options.Mutex == "" && !options.Stats && (options.Station == 0 || options.Year == 0) {
		fmt.Println("No profiling option specified. Use -help to see available options.")
		fmt.Println("\nCommon usage:")
		fmt.Println("  Profile CPU for 30 seconds:")
		fmt.Println("    cimis profile -cpu cpu.prof")
		fmt.Println("\n  Capture heap profile:")
		fmt.Println("    cimis profile -heap heap.prof")
		fmt.Println("\n  Start pprof server:")
		fmt.Println("    cimis profile -server localhost:6060")
		fmt.Println("\n  Print runtime stats:")
		fmt.Println("    cimis profile -stats")
	}
	return nil
}

func profileMemoryDuringIngest(stationID int, year int, dataDir string) {
	fmt.Printf("Profiling memory usage during ingestion of station %d, year %d\n", stationID, year)

	// Get API key
	apiKey := os.Getenv("CIMIS_APP_KEY")
	if apiKey == "" {
		logFatal("CIMIS_APP_KEY environment variable not set")
	}

	// Create API client
	client := newAPIClient(apiKey)
	startDate := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(year, 12, 31, 0, 0, 0, 0, time.UTC)

	// Capture initial memory state
	fmt.Println("\n=== Memory Before Ingestion ===")
	profile.PrintRuntimeStats(os.Stdout)
	initialStats := getProfileMemoryStats()

	// Fetch data
	fmt.Printf("\nFetching daily data for station %d, year %d...\n", stationID, year)
	fetchStart := time.Now()
	apiRecords, err := client.FetchDailyData(stationID, api.FormatCIMISDate(startDate), api.FormatCIMISDate(endDate))
	if err != nil {
		logFatalf("Failed to fetch data: %v", err)
	}
	fetchDuration := time.Since(fetchStart)

	records := api.ConvertDailyToRecords(apiRecords, uint16(stationID))
	if len(records) == 0 {
		fmt.Println("No records to ingest")
		return
	}

	// Allow GC to reclaim apiRecords memory
	apiRecords = nil
	profile.ForceGC()
	time.Sleep(50 * time.Millisecond)

	processStart := time.Now()
	cd := storage.ExtractColumns(records)
	optData, _, err := optimizeColumns(cd, uint16(stationID))
	if err != nil {
		logFatalf("Failed to optimize columns: %v", err)
	}

	compressed, err := compressLevel(optData, 3)
	if err != nil {
		logFatalf("Failed to compress: %v", err)
	}
	processDuration := time.Since(processStart)

	// Write to file
	writeStart := time.Now()
	stationDir := filepath.Join(dataDir, "stations", fmt.Sprintf("%03d", stationID))
	os.MkdirAll(stationDir, 0755)
	chunkPath := filepath.Join(stationDir, fmt.Sprintf("%d_optimized.zst", year))
	os.WriteFile(chunkPath, compressed, 0644)
	writeDuration := time.Since(writeStart)

	// Force GC to get clean memory stats
	profile.ForceGC()
	time.Sleep(100 * time.Millisecond)

	// Capture final memory state
	fmt.Println("\n=== Memory After Ingestion ===")
	profile.PrintRuntimeStats(os.Stdout)
	finalStats := getProfileMemoryStats()

	// Calculate memory usage
	totalDuration := fetchDuration + processDuration + writeDuration
	var memUsed uint64
	if finalStats.Alloc >= initialStats.Alloc {
		memUsed = finalStats.Alloc - initialStats.Alloc
	} else {
		memUsed = 0
	}

	// Print summary
	fmt.Println("\n=== Memory Profiling Summary ===")
	fmt.Printf("Records ingested: %d\n", len(records))
	fmt.Printf("\n--- Timing Breakdown ---\n")
	fmt.Printf("Fetch:     %v\n", fetchDuration)
	fmt.Printf("Process:   %v\n", processDuration)
	fmt.Printf("Write:     %v\n", writeDuration)
	fmt.Printf("Total:     %v\n", totalDuration)
	fmt.Printf("\n--- Memory ---\n")
	fmt.Printf("Memory allocated: %.2f MB\n", float64(memUsed)/(1024*1024))
	fmt.Printf("Bytes per record: %.2f\n", float64(memUsed)/float64(len(records)))
	fmt.Printf("Records per second: %.0f\n", float64(len(records))/totalDuration.Seconds())

	// GC stats
	fmt.Printf("\n--- GC Stats ---\n")
	fmt.Printf("GC runs: %d\n", finalStats.NumGC-initialStats.NumGC)
	if finalStats.NumGC > initialStats.NumGC {
		fmt.Printf("Avg pause: %.3f ms\n", float64(finalStats.PauseNs)/float64(finalStats.NumGC-initialStats.NumGC)/1e6)
	}

	// Data sizes
	originalSize := len(records) * 16
	fmt.Printf("\n--- Data Sizes ---\n")
	fmt.Printf("Original:    %d bytes\n", originalSize)
	fmt.Printf("Optimized:   %d bytes\n", len(optData))
	fmt.Printf("Compressed:  %d bytes\n", len(compressed))
	fmt.Printf("Ratio:       %.2fx\n", float64(originalSize)/float64(len(compressed)))
}
