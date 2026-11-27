package main

import (
	"fmt"
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

type FetchCmd struct {
	Station int  `help:"Station ID" name:"station"`
	Days    int  `help:"Number of days to fetch" name:"days" default:"7"`
	Hourly  bool `help:"Fetch hourly data (default: daily)" name:"hourly"`
}

type FetchStreamingCmd struct {
	Stations    string `help:"CSV list or range (e.g., '2,5,10' or '1-10')" name:"stations"`
	Year        int    `help:"Year to fetch (defaults to current year)" name:"year"`
	Start       string `help:"Start date YYYY-MM-DD (overrides year; MM/DD/YYYY also accepted)" name:"start"`
	End         string `help:"End date YYYY-MM-DD (overrides year; MM/DD/YYYY also accepted)" name:"end"`
	Concurrency int    `help:"Worker pool size" name:"concurrency" default:"4"`
	Gzip        bool   `help:"Enable gzip compression" name:"gzip" default:"true"`
	Format      string `help:"Output format: v1|v2" name:"format" default:"v1"`
	DryRun      bool   `help:"Fetch and decode only, don't write" name:"dry-run"`
	Perf        bool   `help:"Print detailed performance metrics" name:"perf"`
	Allocs      bool   `help:"Measure memory allocations per station (use with concurrency=1)" name:"allocs"`
	Retries     int    `help:"Max retries on failure" name:"retries" default:"3"`
	OutDir      string `help:"Output directory" name:"out"`
}

var (
	retrySleep  = time.Sleep
	retryJitter = func(backoff time.Duration) time.Duration {
		return time.Duration(int64(time.Now().UnixNano()) % int64(backoff/2))
	}
)

func cmdFetch(dataDir, appKey string, args []string) {
	fatalIfErr(runFetch(dataDir, appKey, args))
}

func runFetch(dataDir, appKey string, args []string) error {
	var options FetchCmd
	if err := parseCommandOptions("fetch", args, &options); err != nil {
		return err
	}
	return runFetchOptions(dataDir, appKey, options)
}

func (c *FetchCmd) Run(globals *Globals) error {
	fmt.Fprintln(os.Stderr, "Warning: 'fetch' command is deprecated. Use 'fetch-streaming' for better performance.")
	return runFetchOptions(globals.DataDir, globals.AppKey, *c)
}

func runFetchOptions(dataDir, appKey string, options FetchCmd) error {
	if appKey == "" {
		return fmt.Errorf("CIMIS app key required (use -app-key flag or CIMIS_APP_KEY env var)")
	}
	if options.Station == 0 {
		return fmt.Errorf("station ID required")
	}

	// Calculate date range
	endDate := time.Now()
	startDate := endDate.AddDate(0, 0, -options.Days)

	// Fetch data
	client := newAPIClient(appKey)

	if options.Hourly {
		records, err := client.FetchHourlyData(options.Station, api.FormatCIMISDate(startDate), api.FormatCIMISDate(endDate))
		if err != nil {
			return fmt.Errorf("failed to fetch hourly data: %w", err)
		}
		fmt.Printf("Fetched %d hourly records for station %d\n", len(records), options.Station)
	} else {
		records, err := client.FetchDailyData(options.Station, api.FormatCIMISDate(startDate), api.FormatCIMISDate(endDate))
		if err != nil {
			return fmt.Errorf("failed to fetch daily data: %w", err)
		}
		fmt.Printf("Fetched %d daily records for station %d\n", len(records), options.Station)
	}

	return nil
}

func cmdFetchStreaming(dataDir, appKey string, args []string) {
	fatalIfErr(runFetchStreaming(dataDir, appKey, args))
}

func runFetchStreaming(dataDir, appKey string, args []string) error {
	var options FetchStreamingCmd
	if err := parseCommandOptions("fetch-streaming", args, &options); err != nil {
		return err
	}
	return runFetchStreamingOptions(dataDir, appKey, options)
}

func (c *FetchStreamingCmd) Run(globals *Globals) error {
	return runFetchStreamingOptions(globals.DataDir, globals.AppKey, *c)
}

func runFetchStreamingOptions(dataDir, appKey string, options FetchStreamingCmd) error {
	if appKey == "" {
		appKey = os.Getenv("CIMIS_APP_KEY")
	}
	if appKey == "" {
		return fmt.Errorf("CIMIS app key required (use -app-key flag or CIMIS_APP_KEY env var)")
	}
	if options.Year == 0 {
		options.Year = time.Now().Year()
	}

	if options.Stations == "" {
		return fmt.Errorf("stations required (-stations flag)")
	}

	stationList, err := parseStationList(options.Stations)
	if err != nil {
		return fmt.Errorf("invalid station list: %w", err)
	}

	if len(stationList) == 0 {
		return fmt.Errorf("no stations specified")
	}

	sortStations(stationList)

	var startDate, endDate time.Time
	if options.Start != "" && options.End != "" {
		startDate, err = api.ParseCIMISDate(options.Start)
		if err != nil {
			return fmt.Errorf("invalid start date: %w", err)
		}
		endDate, err = api.ParseCIMISDate(options.End)
		if err != nil {
			return fmt.Errorf("invalid end date: %w", err)
		}
	} else {
		startDate = time.Date(options.Year, 1, 1, 0, 0, 0, 0, time.UTC)
		endDate = time.Date(options.Year, 12, 31, 0, 0, 0, 0, time.UTC)
	}

	if options.Format != "v1" && options.Format != "v2" {
		return fmt.Errorf("format must be v1 or v2")
	}

	outDir := options.OutDir
	if outDir == "" {
		outDir = dataDir
	}
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	dbPath := filepath.Join(outDir, "metadata.sqlite3")
	store, err := metadata.NewStore(dbPath)
	if err != nil {
		return fmt.Errorf("failed to open metadata store: %w", err)
	}
	defer store.Close()

	compressionLevel := 1
	if options.Gzip {
		compressionLevel = 3
	}
	writer, err := newChunkWriter(outDir, compressionLevel)
	if err != nil {
		return fmt.Errorf("failed to create chunk writer: %w", err)
	}

	client := newOptimizedAPIClient(appKey)

	type job struct {
		stationID uint16
	}

	jobs := make(chan job, len(stationList))
	results := make(chan stationFetchResult, len(stationList))

	for w := 0; w < options.Concurrency; w++ {
		go func() {
			for j := range jobs {
				m := fetchStationStreaming(
					client, store, writer, j.stationID,
					startDate, endDate, options.Format, options.DryRun, options.Retries,
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

	if options.Perf {
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

	if options.Allocs {
		fmt.Println("\nNote: Allocation tracking enabled (authoritative when concurrency=1)")
	}

	return nil
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
			retrySleep(backoff + retryJitter(backoff))
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

		if err := saveChunkMetadata(store, &types.ChunkInfo{
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
