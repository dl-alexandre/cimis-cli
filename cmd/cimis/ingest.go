package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/dl-alexandre/cimis-cli/internal/api"
	"github.com/dl-alexandre/cimis-tsdb/metadata"
)

type IngestCmd struct {
	Station     int `help:"Station ID" name:"station"`
	Year        int `help:"Year to ingest (defaults to current year)" name:"year"`
	Compression int `help:"Compression level (1-16)" name:"compression" default:"1"`
}

func runIngest(dataDir, appKey string, args []string) error {
	var options IngestCmd
	if err := parseCommandOptions("ingest", args, &options); err != nil {
		return err
	}
	return runIngestOptions(dataDir, appKey, options)
}

func (c *IngestCmd) Run(globals *Globals) error {
	return runIngestOptions(globals.DataDir, globals.AppKey, *c)
}

func runIngestOptions(dataDir, appKey string, options IngestCmd) error {
	if appKey == "" {
		return fmt.Errorf("CIMIS app key required")
	}
	if options.Station == 0 {
		return fmt.Errorf("station ID required")
	}

	if options.Year == 0 {
		options.Year = time.Now().Year()
	}

	// Initialize components
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	dbPath := filepath.Join(dataDir, "metadata.sqlite3")
	store, err := metadata.NewStore(dbPath)
	if err != nil {
		return fmt.Errorf("failed to open metadata store: %w", err)
	}
	defer store.Close()

	writer, err := newChunkWriter(dataDir, options.Compression)
	if err != nil {
		return fmt.Errorf("failed to create chunk writer: %w", err)
	}

	// Check if chunk already exists
	exists, _ := store.ChunkExists(uint16(options.Station), options.Year, "daily")
	if exists {
		fmt.Printf("Chunk for station %d year %d already exists. Skipping.\n", options.Station, options.Year)
		return nil
	}

	// Fetch daily data for the year using optimized streaming client
	client := newOptimizedAPIClient(appKey)
	startDate := time.Date(options.Year, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(options.Year, 12, 31, 0, 0, 0, 0, time.UTC)

	fmt.Printf("Fetching daily data for station %d, year %d...\n", options.Station, options.Year)
	records, fetchMetrics, err := client.FetchDailyDataStreaming(options.Station, api.FormatCIMISDate(startDate), api.FormatCIMISDate(endDate))
	if err != nil {
		return fmt.Errorf("failed to fetch data: %w", err)
	}

	if fetchMetrics != nil {
		fmt.Printf("  Fetch: %v (DNS: %v, TCP: %v, TLS: %v, TTFB: %v)\n",
			fetchMetrics.TotalDuration, fetchMetrics.DNSLookup, fetchMetrics.TCPConnect,
			fetchMetrics.TLSHandshake, fetchMetrics.TTFB)
	}

	if len(records) == 0 {
		fmt.Printf("No records found for station %d, year %d. The station may not have data for this period.\n", options.Station, options.Year)
		return nil
	}

	// Write chunk
	chunkInfo, err := writer.WriteDailyChunk(uint16(options.Station), options.Year, records)
	if err != nil {
		return fmt.Errorf("failed to write chunk: %w", err)
	}

	// Save metadata
	if err := saveChunkMetadata(store, chunkInfo); err != nil {
		return fmt.Errorf("failed to save chunk metadata: %w", err)
	}

	// Print summary
	fmt.Printf("Ingested %d daily records\n", len(records))
	fmt.Printf("  Compressed: %d bytes (%.2fx ratio)\n", chunkInfo.FileSize, chunkInfo.CompressionRatio)
	fmt.Printf("  Stored in: %s\n", chunkInfo.FilePath)
	return nil
}
