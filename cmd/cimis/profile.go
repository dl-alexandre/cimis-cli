// Profile command for the CIMIS database CLI.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/dl-alexandre/cimis-cli/internal/api"
	"github.com/dl-alexandre/cimis-cli/internal/profile"
	"github.com/dl-alexandre/cimis-tsdb/storage"
)

func cmdProfile(dataDir string, args []string) {
	fs := flag.NewFlagSet("profile", flag.ExitOnError)
	cpu := fs.String("cpu", "", "CPU profile output file")
	heap := fs.String("heap", "", "Heap profile output file")
	allocs := fs.String("allocs", "", "Allocations profile output file")
	goroutines := fs.String("goroutines", "", "Goroutine profile output file")
	mutex := fs.String("mutex", "", "Mutex profile output file")
	duration := fs.Duration("duration", 30*time.Second, "Profiling duration")
	server := fs.String("server", "", "Start pprof server on address (e.g., localhost:6060)")
	stats := fs.Bool("stats", false, "Print runtime statistics")
	ingestStation := fs.Int("station", 0, "Station ID for memory profiling during ingest")
	ingestYear := fs.Int("year", 0, "Year for memory profiling during ingest")

	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}

	// Start pprof server if requested
	if *server != "" {
		profile.StartPProfServer(*server)
		fmt.Printf("pprof server started on %s\n", *server)
		fmt.Println("Press Ctrl+C to stop...")

		// Wait for interrupt
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		fmt.Println("\nShutting down...")
		return
	}

	// Print runtime stats
	if *stats {
		profile.PrintRuntimeStats(os.Stdout)
		return
	}

	profiler := profile.NewProfiler()

	// CPU profiling
	if *cpu != "" {
		fmt.Printf("Starting CPU profile for %v...\n", *duration)
		if err := profiler.StartCPUProfile(*cpu); err != nil {
			log.Fatalf("Failed to start CPU profile: %v", err)
		}

		time.Sleep(*duration)

		if err := profiler.StopCPUProfile(); err != nil {
			log.Fatalf("Failed to stop CPU profile: %v", err)
		}
		fmt.Printf("CPU profile written to: %s\n", *cpu)
	}

	// Heap profiling
	if *heap != "" {
		if err := profiler.WriteHeapProfile(*heap); err != nil {
			log.Fatalf("Failed to write heap profile: %v", err)
		}
		fmt.Printf("Heap profile written to: %s\n", *heap)
	}

	// Allocs profiling
	if *allocs != "" {
		if err := profiler.ProfileAllocs(*allocs); err != nil {
			log.Fatalf("Failed to write allocs profile: %v", err)
		}
		fmt.Printf("Allocs profile written to: %s\n", *allocs)
	}

	// Goroutine profiling
	if *goroutines != "" {
		if err := profiler.ProfileGoroutines(*goroutines); err != nil {
			log.Fatalf("Failed to write goroutine profile: %v", err)
		}
		fmt.Printf("Goroutine profile written to: %s\n", *goroutines)
	}

	// Mutex profiling
	if *mutex != "" {
		profile.EnableMutexProfiling(1)
		time.Sleep(*duration)
		if err := profiler.ProfileMutex(*mutex); err != nil {
			log.Fatalf("Failed to write mutex profile: %v", err)
		}
		fmt.Printf("Mutex profile written to: %s\n", *mutex)
	}

	// Memory profiling during ingestion
	if *ingestStation > 0 && *ingestYear > 0 {
		profileMemoryDuringIngest(*ingestStation, *ingestYear, dataDir)
		return
	}

	// If no specific profile requested, print stats
	if *cpu == "" && *heap == "" && *allocs == "" && *goroutines == "" && *mutex == "" && !*stats && (*ingestStation == 0 || *ingestYear == 0) {
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
}

func profileMemoryDuringIngest(stationID int, year int, dataDir string) {
	fmt.Printf("Profiling memory usage during ingestion of station %d, year %d\n", stationID, year)

	// Get API key
	apiKey := os.Getenv("CIMIS_APP_KEY")
	if apiKey == "" {
		log.Fatal("CIMIS_APP_KEY environment variable not set")
	}

	// Create API client
	client := api.NewClient(apiKey)
	startDate := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(year, 12, 31, 0, 0, 0, 0, time.UTC)

	// Capture initial memory state
	fmt.Println("\n=== Memory Before Ingestion ===")
	profile.PrintRuntimeStats(os.Stdout)
	initialStats := profile.GetMemoryStats()

	// Fetch data
	fmt.Printf("\nFetching daily data for station %d, year %d...\n", stationID, year)
	fetchStart := time.Now()
	apiRecords, err := client.FetchDailyData(stationID, api.FormatCIMISDate(startDate), api.FormatCIMISDate(endDate))
	if err != nil {
		log.Fatalf("Failed to fetch data: %v", err)
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
	optData, _, err := storage.OptimizeColumns(cd, uint16(stationID))
	if err != nil {
		log.Fatalf("Failed to optimize columns: %v", err)
	}

	compressed, err := storage.CompressLevel(optData, 3)
	if err != nil {
		log.Fatalf("Failed to compress: %v", err)
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
	finalStats := profile.GetMemoryStats()

	// Calculate memory usage
	totalDuration := fetchDuration + processDuration + writeDuration
	memUsed := finalStats.Alloc - initialStats.Alloc
	if memUsed < 0 {
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
