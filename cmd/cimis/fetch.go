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
