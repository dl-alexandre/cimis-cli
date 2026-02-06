// Main entry point for the cimisdb CLI.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dl-alexandre/cimis-cli/internal/api"
	"github.com/dl-alexandre/cimis-tsdb/metadata"
	"github.com/dl-alexandre/cimis-tsdb/storage"
	"github.com/dl-alexandre/cimis-tsdb/types"
)

var (
	// Version is set during build
	Version = "dev"
	// BuildTime is set during build
	BuildTime = "unknown"
)

// parseCacheSize parses cache size strings like "100MB", "1GB" to bytes.
// Returns the size in bytes, or 0 if parsing fails.
func parseCacheSize(sizeStr string) int64 {
	if sizeStr == "" {
		return 0
	}

	sizeStr = strings.TrimSpace(sizeStr)
	sizeStr = strings.ToUpper(sizeStr)

	// Try to parse with suffix
	if strings.HasSuffix(sizeStr, "GB") {
		numStr := strings.TrimSuffix(sizeStr, "GB")
		if num, err := strconv.ParseFloat(numStr, 64); err == nil {
			return int64(num * 1024 * 1024 * 1024)
		}
	} else if strings.HasSuffix(sizeStr, "MB") {
		numStr := strings.TrimSuffix(sizeStr, "MB")
		if num, err := strconv.ParseFloat(numStr, 64); err == nil {
			return int64(num * 1024 * 1024)
		}
	} else if strings.HasSuffix(sizeStr, "KB") {
		numStr := strings.TrimSuffix(sizeStr, "KB")
		if num, err := strconv.ParseFloat(numStr, 64); err == nil {
			return int64(num * 1024)
		}
	} else {
		// Try to parse as plain bytes
		if num, err := strconv.ParseInt(sizeStr, 10, 64); err == nil {
			return num
		}
	}

	return 0
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	// Global flags
	dataDir := flag.String("data-dir", "./data", "Data directory path")
	appKey := flag.String("app-key", os.Getenv("CIMIS_APP_KEY"), "CIMIS API app key")

	// Subcommands
	switch os.Args[1] {
	case "version":
		fmt.Printf("cimisdb version %s (built %s)\n", Version, BuildTime)

	case "init":
		cmdInit(*dataDir)

	case "fetch":
		fmt.Fprintln(os.Stderr, "Warning: 'fetch' command is deprecated. Use 'fetch-streaming' for better performance.")
		cmdFetch(*dataDir, *appKey, os.Args[2:])

	case "fetch-streaming":
		cmdFetchStreaming(*dataDir, *appKey, os.Args[2:])

	case "ingest":
		cmdIngest(*dataDir, *appKey, os.Args[2:])

	case "ingest-opt":
		cmdIngestOptimized(*dataDir, *appKey, os.Args[2:])

	case "query":
		cmdQuery(*dataDir, os.Args[2:])

	case "stats":
		cmdStats(*dataDir)

	case "verify":
		cmdVerify(*dataDir)

	case "profile":
		cmdProfile(*dataDir, os.Args[2:])

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`Usage: cimisdb <command> [options]

Commands:
  version          Show version information
  init             Initialize database directories and metadata
  fetch            Fetch data from CIMIS API (DEPRECATED: use fetch-streaming)
  fetch-streaming  Fetch with optimized streaming + detailed metrics
  ingest           Fetch and store using streaming (production default)
  query            Query stored data
  stats            Show database statistics
  verify           Verify chunk integrity
  profile          CPU, memory, and performance profiling

Global Options:
  -data-dir string    Data directory (default: ./data)
  -app-key string     CIMIS API app key (or CIMIS_APP_KEY env var)

Examples:
   # Initialize database
   cimisdb init

   # Fetch recent data for station 2
   cimisdb fetch -station 2 -days 30

   # Fetch multiple stations with streaming and detailed metrics
   cimisdb fetch-streaming -stations 2,5,10 -year 2024 -concurrency 8 -perf

   # Ingest data for a specific year
   cimisdb ingest -station 2 -year 2020

   # Query June 2020 data
   cimisdb query -station 2 -start 2020-06-01 -end 2020-06-30

   # Query with caching and performance metrics
   cimisdb query -station 2 -start 2020-06-01 -end 2020-06-30 -cache 100MB -perf`)
}

func cmdInit(dataDir string) {
	// Create directories
	dirs := []string{
		filepath.Join(dataDir, "stations"),
		filepath.Join(dataDir, "spatial"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatalf("Failed to create directory %s: %v", dir, err)
		}
	}

	// Initialize metadata database
	dbPath := filepath.Join(dataDir, "metadata.sqlite3")
	store, err := metadata.NewStore(dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize metadata database: %v", err)
	}
	defer store.Close()

	fmt.Println("Database initialized successfully")
	fmt.Printf("Data directory: %s\n", dataDir)
	fmt.Printf("Metadata: %s\n", dbPath)
}

func cmdFetch(dataDir, appKey string, args []string) {
	if appKey == "" {
		log.Fatal("CIMIS app key required (use -app-key flag or CIMIS_APP_KEY env var)")
	}

	// Parse flags
	fs := flag.NewFlagSet("fetch", flag.ExitOnError)
	stationID := fs.Int("station", 0, "Station ID")
	days := fs.Int("days", 7, "Number of days to fetch")
	hourly := fs.Bool("hourly", false, "Fetch hourly data (default: daily)")

	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}

	if *stationID == 0 {
		log.Fatal("Station ID required")
	}

	// Calculate date range
	endDate := time.Now()
	startDate := endDate.AddDate(0, 0, -*days)

	// Fetch data
	client := api.NewClient(appKey)

	if *hourly {
		records, err := client.FetchHourlyData(*stationID, api.FormatCIMISDate(startDate), api.FormatCIMISDate(endDate))
		if err != nil {
			log.Fatalf("Failed to fetch hourly data: %v", err)
		}
		fmt.Printf("Fetched %d hourly records for station %d\n", len(records), *stationID)
	} else {
		records, err := client.FetchDailyData(*stationID, api.FormatCIMISDate(startDate), api.FormatCIMISDate(endDate))
		if err != nil {
			log.Fatalf("Failed to fetch daily data: %v", err)
		}
		fmt.Printf("Fetched %d daily records for station %d\n", len(records), *stationID)
	}
}

func cmdIngest(dataDir, appKey string, args []string) {
	if appKey == "" {
		log.Fatal("CIMIS app key required")
	}

	// Parse flags
	fs := flag.NewFlagSet("ingest", flag.ExitOnError)
	stationID := fs.Int("station", 0, "Station ID")
	year := fs.Int("year", 0, "Year to ingest (default: current year)")
	compressionLevel := fs.Int("compression", 1, "Compression level (1-16)")

	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}

	if *stationID == 0 {
		log.Fatal("Station ID required")
	}

	if *year == 0 {
		*year = time.Now().Year()
	}

	// Initialize components
	dbPath := filepath.Join(dataDir, "metadata.sqlite3")
	store, err := metadata.NewStore(dbPath)
	if err != nil {
		log.Fatalf("Failed to open metadata store: %v", err)
	}
	defer store.Close()

	writer, err := storage.NewChunkWriter(dataDir, *compressionLevel)
	if err != nil {
		log.Fatalf("Failed to create chunk writer: %v", err)
	}

	// Check if chunk already exists
	exists, _ := store.ChunkExists(uint16(*stationID), *year, "daily")
	if exists {
		fmt.Printf("Chunk for station %d year %d already exists. Skipping.\n", *stationID, *year)
		return
	}

	// Fetch daily data for the year using optimized streaming client
	client := api.NewOptimizedClient(appKey)
	startDate := time.Date(*year, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(*year, 12, 31, 0, 0, 0, 0, time.UTC)

	fmt.Printf("Fetching daily data for station %d, year %d...\n", *stationID, *year)
	records, fetchMetrics, err := client.FetchDailyDataStreaming(*stationID, api.FormatCIMISDate(startDate), api.FormatCIMISDate(endDate))
	if err != nil {
		log.Fatalf("Failed to fetch data: %v", err)
	}

	if fetchMetrics != nil {
		fmt.Printf("  Fetch: %v (DNS: %v, TCP: %v, TLS: %v, TTFB: %v)\n",
			fetchMetrics.TotalDuration, fetchMetrics.DNSLookup, fetchMetrics.TCPConnect,
			fetchMetrics.TLSHandshake, fetchMetrics.TTFB)
	}

	if len(records) == 0 {
		fmt.Println("No records to ingest")
		return
	}

	// Write chunk
	chunkInfo, err := writer.WriteDailyChunk(uint16(*stationID), *year, records)
	if err != nil {
		log.Fatalf("Failed to write chunk: %v", err)
	}

	// Save metadata
	if err := store.SaveChunk(chunkInfo); err != nil {
		log.Fatalf("Failed to save chunk metadata: %v", err)
	}

	// Print summary
	fmt.Printf("Ingested %d daily records\n", len(records))
	fmt.Printf("  Compressed: %d bytes (%.2fx ratio)\n", chunkInfo.FileSize, chunkInfo.CompressionRatio)
	fmt.Printf("  Stored in: %s\n", chunkInfo.FilePath)
}

func cmdQuery(dataDir string, args []string) {
	// Parse flags
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	stationID := fs.Int("station", 0, "Station ID")
	startDate := fs.String("start", "", "Start date (YYYY-MM-DD)")
	endDate := fs.String("end", "", "End date (YYYY-MM-DD)")
	hourly := fs.Bool("hourly", false, "Query hourly data (default: daily)")
	perf := fs.Bool("perf", false, "Show performance metrics")
	cache := fs.String("cache", "", "Enable caching with specified size (e.g., 100MB, 1GB)")

	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}

	if *stationID == 0 {
		log.Fatal("Station ID required")
	}

	// Start total query timer
	queryStart := time.Now()

	// Parse dates
	start, err := time.Parse("2006-01-02", *startDate)
	if err != nil {
		log.Fatalf("Invalid start date: %v", err)
	}
	end, err := time.Parse("2006-01-02", *endDate)
	if err != nil {
		log.Fatalf("Invalid end date: %v", err)
	}

	// Initialize metadata store
	dbPath := filepath.Join(dataDir, "metadata.sqlite3")
	store, err := metadata.NewStore(dbPath)
	if err != nil {
		log.Fatalf("Failed to open metadata store: %v", err)
	}
	defer store.Close()

	// Initialize chunk reader (with caching if requested)
	var reader interface {
		ReadDailyChunk(stationID uint16, year int) ([]types.DailyRecord, error)
		ReadHourlyChunk(stationID uint16, year int) ([]types.HourlyRecord, error)
	}
	var cachedReader *storage.CachedChunkReader

	if *cache != "" {
		cacheSize := parseCacheSize(*cache)
		if cacheSize <= 0 {
			log.Fatalf("Invalid cache size: %s", *cache)
		}
		cachedReader = storage.NewCachedChunkReader(dataDir, cacheSize)
		reader = cachedReader
	} else {
		reader = storage.NewChunkReader(dataDir)
	}

	// Get chunks in range
	startYear := start.Year()
	endYear := end.Year()
	dataType := types.DataTypeDaily
	if *hourly {
		dataType = types.DataTypeHourly
	}

	// Time metadata lookup
	metadataStart := time.Now()
	chunks, err := store.GetChunksForYearRange(uint16(*stationID), startYear, endYear, dataType)
	metadataDuration := time.Since(metadataStart)
	if err != nil {
		log.Fatalf("Failed to get chunks: %v", err)
	}

	if len(chunks) == 0 {
		fmt.Printf("No data found for station %d in range %s to %s\n", *stationID, *startDate, *endDate)
		return
	}

	// Read and filter records
	fmt.Printf("Querying %d chunks...\n", len(chunks))

	var totalRecords int
	var chunksRead int
	var totalChunkReadTime time.Duration
	var totalFilterTime time.Duration

	for _, chunk := range chunks {
		if *hourly {
			// Time chunk read
			chunkReadStart := time.Now()
			records, err := reader.ReadHourlyChunk(chunk.StationID, chunk.Year)
			chunkReadDuration := time.Since(chunkReadStart)
			totalChunkReadTime += chunkReadDuration
			chunksRead++

			if err != nil {
				log.Printf("Warning: failed to read chunk %d: %v", chunk.Year, err)
				continue
			}
			// Filter by timestamp range
			filterStart := time.Now()
			startTs := uint32(start.Sub(time.Date(1985, 1, 1, 0, 0, 0, 0, time.UTC)).Hours())
			endTs := uint32(end.Sub(time.Date(1985, 1, 1, 0, 0, 0, 0, time.UTC)).Hours())

			for _, r := range records {
				if r.Timestamp >= startTs && r.Timestamp < endTs {
					totalRecords++
					if totalRecords <= 10 {
						ts := time.Date(1985, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(r.Timestamp) * time.Hour)
						fmt.Printf("  %s: Temp=%.1f°C ET=%.2fmm Wind=%.1fm/s Humidity=%d%%\n",
							ts.Format("2006-01-02 15:00"),
							float64(r.Temperature)/10.0,
							float64(r.ET)/1000.0,
							float64(r.WindSpeed)/10.0,
							r.Humidity)
					}
				}
			}
			totalFilterTime += time.Since(filterStart)
		} else {
			// Time chunk read
			chunkReadStart := time.Now()
			records, err := reader.ReadDailyChunk(chunk.StationID, chunk.Year)
			chunkReadDuration := time.Since(chunkReadStart)
			totalChunkReadTime += chunkReadDuration
			chunksRead++

			if err != nil {
				log.Printf("Warning: failed to read chunk %d: %v", chunk.Year, err)
				continue
			}
			// Filter by timestamp range
			filterStart := time.Now()
			startTs := uint32(start.Sub(time.Date(1985, 1, 1, 0, 0, 0, 0, time.UTC)).Hours() / 24)
			endTs := uint32(end.Sub(time.Date(1985, 1, 1, 0, 0, 0, 0, time.UTC)).Hours() / 24)

			for _, r := range records {
				if r.Timestamp >= startTs && r.Timestamp < endTs {
					totalRecords++
					if totalRecords <= 10 {
						ts := time.Date(1985, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(r.Timestamp) * 24 * time.Hour)
						fmt.Printf("  %s: Temp=%.1f°C ET=%.2fmm Wind=%.1fm/s Humidity=%d%%\n",
							ts.Format("2006-01-02"),
							float64(r.Temperature)/10.0,
							float64(r.ET)/100.0,
							float64(r.WindSpeed)/10.0,
							r.Humidity)
					}
				}
			}
			totalFilterTime += time.Since(filterStart)
		}
	}

	fmt.Printf("\nTotal records: %d\n", totalRecords)
	if totalRecords > 10 {
		fmt.Printf("(showing first 10)\n")
	}

	// Print performance metrics if requested
	if *perf {
		totalDuration := time.Since(queryStart)
		avgChunkReadTime := time.Duration(0)
		if chunksRead > 0 {
			avgChunkReadTime = totalChunkReadTime / time.Duration(chunksRead)
		}
		avgRecordTime := time.Duration(0)
		if totalRecords > 0 {
			avgRecordTime = totalFilterTime / time.Duration(totalRecords)
		}
		recordsPerSec := float64(0)
		if totalDuration.Seconds() > 0 {
			recordsPerSec = float64(totalRecords) / totalDuration.Seconds()
		}

		fmt.Println("\n=== Performance Metrics ===")
		fmt.Printf("Total query duration:      %v\n", totalDuration)
		fmt.Printf("Metadata lookup time:      %v\n", metadataDuration)
		fmt.Printf("Chunks read:               %d\n", chunksRead)
		fmt.Printf("Average chunk read time:   %v\n", avgChunkReadTime)
		fmt.Printf("Total filter/process time: %v\n", totalFilterTime)
		fmt.Printf("Average record time:       %v\n", avgRecordTime)
		fmt.Printf("Records per second:        %.2f\n", recordsPerSec)

		// Print cache statistics if caching was enabled
		if cachedReader != nil {
			cacheStats := cachedReader.GetCacheStats()
			fmt.Println("\n=== Cache Statistics ===")
			fmt.Println(storage.FormatCacheStats(cacheStats))
		}
	}
}

func cmdStats(dataDir string) {
	dbPath := filepath.Join(dataDir, "metadata.sqlite3")
	store, err := metadata.NewStore(dbPath)
	if err != nil {
		log.Fatalf("Failed to open metadata store: %v", err)
	}
	defer store.Close()

	stats, err := store.GetDatabaseStats()
	if err != nil {
		log.Fatalf("Failed to get stats: %v", err)
	}

	fmt.Println("Database Statistics")
	fmt.Println("===================")
	fmt.Printf("Stations:           %d\n", stats["station_count"])
	fmt.Printf("Active stations:    %d\n", stats["active_station_count"])
	fmt.Printf("Total chunks:      %d\n", stats["chunk_count"])
	fmt.Printf("Total rows:        %d\n", stats["total_rows"])
	fmt.Printf("Compressed size:   %.2f MB\n", float64(stats["total_compressed_bytes"].(int64))/(1024*1024))
	fmt.Printf("Avg compression:   %.2fx\n", stats["avg_compression_ratio"])
}

func cmdVerify(dataDir string) {
	// Walk data directory
	stationsDir := filepath.Join(dataDir, "stations")
	entries, err := os.ReadDir(stationsDir)
	if err != nil {
		log.Fatalf("Failed to read stations directory: %v", err)
	}

	var verified, failed int

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		stationDir := filepath.Join(stationsDir, entry.Name())
		chunks, err := os.ReadDir(stationDir)
		if err != nil {
			continue
		}

		// Parse station ID from directory name
		stationID, _ := strconv.Atoi(entry.Name())

		for _, chunk := range chunks {
			if chunk.IsDir() || filepath.Ext(chunk.Name()) != ".zst" {
				continue
			}

			// Try to read and decompress
			filePath := filepath.Join(stationDir, chunk.Name())
			compressed, err := os.ReadFile(filePath)
			if err != nil {
				fmt.Printf("FAIL: %s - read error: %v\n", filePath, err)
				failed++
				continue
			}

			_, err = storage.Decompress(nil, compressed)
			if err != nil {
				fmt.Printf("FAIL: %s - decompress error: %v\n", filePath, err)
				failed++
				continue
			}

			fmt.Printf("OK: %s (station %d)\n", filePath, stationID)
			verified++
		}
	}

	fmt.Printf("\nVerification complete: %d OK, %d failed\n", verified, failed)
	if failed > 0 {
		os.Exit(1)
	}
}

func cmdFetchStreaming(dataDir, appKey string, args []string) {
	if appKey == "" {
		appKey = os.Getenv("CIMIS_APP_KEY")
	}
	if appKey == "" {
		log.Fatal("CIMIS app key required (use -app-key flag or CIMIS_APP_KEY env var)")
	}

	fs := flag.NewFlagSet("fetch-streaming", flag.ExitOnError)
	stations := fs.String("stations", "", "CSV list or range (e.g., '2,5,10' or '1-10')")
	year := fs.Int("year", time.Now().Year(), "Year to fetch")
	startStr := fs.String("start", "", "Start date MM/DD/YYYY (overrides year)")
	endStr := fs.String("end", "", "End date MM/DD/YYYY (overrides year)")
	concurrency := fs.Int("concurrency", 4, "Worker pool size")
	gzip := fs.Bool("gzip", true, "Enable gzip compression")
	format := fs.String("format", "v1", "Output format: v1|v2")
	dryRun := fs.Bool("dry-run", false, "Fetch and decode only, don't write")
	perf := fs.Bool("perf", false, "Print detailed performance metrics")
	allocs := fs.Bool("allocs", false, "Measure memory allocations per station (use with concurrency=1)")
	retries := fs.Int("retries", 3, "Max retries on failure")
	outDir := fs.String("out", dataDir, "Output directory")

	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}

	if *stations == "" {
		log.Fatal("Stations required (-stations flag)")
	}

	stationList, err := parseStationList(*stations)
	if err != nil {
		log.Fatalf("Invalid station list: %v", err)
	}

	if len(stationList) == 0 {
		log.Fatal("No stations specified")
	}

	sortStations(stationList)

	var startDate, endDate time.Time
	if *startStr != "" && *endStr != "" {
		startDate, err = time.Parse("01/02/2006", *startStr)
		if err != nil {
			log.Fatalf("Invalid start date: %v", err)
		}
		endDate, err = time.Parse("01/02/2006", *endStr)
		if err != nil {
			log.Fatalf("Invalid end date: %v", err)
		}
	} else {
		startDate = time.Date(*year, 1, 1, 0, 0, 0, 0, time.UTC)
		endDate = time.Date(*year, 12, 31, 0, 0, 0, 0, time.UTC)
	}

	if *format != "v1" && *format != "v2" {
		log.Fatal("Format must be v1 or v2")
	}

	dbPath := filepath.Join(*outDir, "metadata.sqlite3")
	store, err := metadata.NewStore(dbPath)
	if err != nil {
		log.Fatalf("Failed to open metadata store: %v", err)
	}
	defer store.Close()

	compressionLevel := 1
	if *gzip {
		compressionLevel = 3
	}
	writer, err := storage.NewChunkWriter(*outDir, compressionLevel)
	if err != nil {
		log.Fatalf("Failed to create chunk writer: %v", err)
	}

	client := api.NewOptimizedClient(appKey)

	type job struct {
		stationID uint16
	}

	jobs := make(chan job, len(stationList))
	results := make(chan stationFetchResult, len(stationList))

	for w := 0; w < *concurrency; w++ {
		go func() {
			for j := range jobs {
				m := fetchStationStreaming(
					client, store, writer, j.stationID,
					startDate, endDate, *format, *dryRun, *retries,
				)
				results <- m
			}
		}()
	}

	for _, sid := range stationList {
		jobs <- job{stationID: uint16(sid)}
	}
	close(jobs)

	var allMetrics []stationFetchResult
	var successCount, failCount int
	var totalRecords int

	for i := 0; i < len(stationList); i++ {
		m := <-results
		allMetrics = append(allMetrics, m)
		if m.success {
			successCount++
			totalRecords += m.recordCount
		} else {
			failCount++
		}
	}

	fmt.Printf("\n=== Fetch Streaming Summary ===\n")
	fmt.Printf("Stations processed: %d\n", len(stationList))
	fmt.Printf("Successful: %d\n", successCount)
	fmt.Printf("Failed: %d\n", failCount)
	fmt.Printf("Total records: %d\n", totalRecords)

	if *perf {
		fmt.Println("\n=== Performance Metrics ===")
		for _, m := range allMetrics {
			if m.success {
				fmt.Printf("Station %d:\n", m.stationID)
				fmt.Printf("  Records: %d\n", m.recordCount)
				fmt.Printf("  DNS:     %v\n", m.dns)
				fmt.Printf("  TCP:     %v\n", m.tcp)
				fmt.Printf("  TLS:     %v\n", m.tls)
				fmt.Printf("  TTFB:    %v\n", m.ttfb)
				fmt.Printf("  Read:    %v\n", m.read)
				fmt.Printf("  Decode:  %v\n", m.decode)
				fmt.Printf("  Write:   %v\n", m.write)
				fmt.Printf("  Total:   %v\n", m.totalTime)
			} else {
				fmt.Printf("Station %d: FAILED - %v\n", m.stationID, m.err)
			}
		}
	}

	if failCount > 0 {
		fmt.Printf("\nWarning: %d station(s) failed\n", failCount)
	}

	if *allocs {
		fmt.Println("\nNote: Allocation tracking enabled (authoritative when concurrency=1)")
	}
}

func parseStationList(input string) ([]int, error) {
	var stations []int
	parts := strings.Split(input, ",")

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if strings.Contains(part, "-") {
			rangeParts := strings.Split(part, "-")
			if len(rangeParts) != 2 {
				return nil, fmt.Errorf("invalid range format: %s", part)
			}
			start, err := strconv.Atoi(strings.TrimSpace(rangeParts[0]))
			if err != nil {
				return nil, fmt.Errorf("invalid range start: %s", rangeParts[0])
			}
			end, err := strconv.Atoi(strings.TrimSpace(rangeParts[1]))
			if err != nil {
				return nil, fmt.Errorf("invalid range end: %s", rangeParts[1])
			}
			for i := start; i <= end; i++ {
				stations = append(stations, i)
			}
		} else {
			sid, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("invalid station ID: %s", part)
			}
			stations = append(stations, sid)
		}
	}

	return stations, nil
}

func sortStations(stations []int) {
	for i := 0; i < len(stations)-1; i++ {
		for j := i + 1; j < len(stations); j++ {
			if stations[j] < stations[i] {
				stations[i], stations[j] = stations[j], stations[i]
			}
		}
	}
}

type stationFetchResult struct {
	stationID    uint16
	success      bool
	recordCount  int
	dns          time.Duration
	tcp          time.Duration
	tls          time.Duration
	ttfb         time.Duration
	read         time.Duration
	decode       time.Duration
	write        time.Duration
	totalTime    time.Duration
	allocMetrics *AllocMetrics
	err          error
}

func fetchStationStreaming(
	client *api.OptimizedClient,
	store *metadata.Store,
	writer *storage.ChunkWriter,
	stationID uint16,
	startDate, endDate time.Time,
	format string,
	dryRun bool,
	maxRetries int,
) stationFetchResult {
	m := stationFetchResult{stationID: stationID}
	totalStart := time.Now()

	year := startDate.Year()
	exists, _ := store.ChunkExists(stationID, year, types.DataTypeDaily)
	if exists {
		m.success = true
		m.recordCount = 0
		m.totalTime = time.Since(totalStart)
		return m
	}

	var records []types.DailyRecord
	var err error
	var fetchMetrics *api.FetchMetrics

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<uint(attempt)) * time.Second
			jitter := time.Duration(int64(time.Now().UnixNano()) % int64(backoff/2))
			time.Sleep(backoff + jitter)
		}

		records, fetchMetrics, err = client.FetchDailyDataStreaming(
			int(stationID),
			api.FormatCIMISDate(startDate),
			api.FormatCIMISDate(endDate),
		)

		if err == nil {
			break
		}

		if attempt < maxRetries {
			continue
		}
	}

	if err != nil {
		m.success = false
		m.err = err
		m.totalTime = time.Since(totalStart)
		return m
	}

	if fetchMetrics != nil {
		m.dns = fetchMetrics.DNSLookup
		m.tcp = fetchMetrics.TCPConnect
		m.tls = fetchMetrics.TLSHandshake
		m.ttfb = fetchMetrics.TTFB
		m.read = fetchMetrics.BodyRead
		m.decode = fetchMetrics.JSONDecode
	}
	m.recordCount = len(records)

	if !dryRun && len(records) > 0 {
		writeStart := time.Now()
		_, err := writer.WriteDailyChunk(stationID, year, records)
		m.write = time.Since(writeStart)

		if err != nil {
			m.success = false
			m.err = err
			m.totalTime = time.Since(totalStart)
			return m
		}

		if err := store.SaveChunk(&types.ChunkInfo{
			StationID: stationID,
			Year:      year,
			DataType:  types.DataTypeDaily,
		}); err != nil {
			m.success = false
			m.err = err
			m.totalTime = time.Since(totalStart)
			return m
		}
	}

	m.success = true
	m.totalTime = time.Since(totalStart)
	return m
}
