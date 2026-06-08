package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/dl-alexandre/cimis-cli/internal/api"
	"github.com/dl-alexandre/cimis-tsdb/metadata"
)

func runIngest(dataDir, appKey string, args []string) error {
	if appKey == "" {
		return fmt.Errorf("CIMIS app key required")
	}

	// Parse flags
	fs := flag.NewFlagSet("ingest", flag.ContinueOnError)
	stationID := fs.Int("station", 0, "Station ID")
	year := fs.Int("year", 0, "Year to ingest (default: current year)")
	compressionLevel := fs.Int("compression", 1, "Compression level (1-16)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *stationID == 0 {
		return fmt.Errorf("station ID required")
	}

	if *year == 0 {
		*year = time.Now().Year()
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

	writer, err := newChunkWriter(dataDir, *compressionLevel)
	if err != nil {
		return fmt.Errorf("failed to create chunk writer: %w", err)
	}

	// Check if chunk already exists
	exists, _ := store.ChunkExists(uint16(*stationID), *year, "daily")
	if exists {
		fmt.Printf("Chunk for station %d year %d already exists. Skipping.\n", *stationID, *year)
		return nil
	}

	// Fetch daily data for the year using optimized streaming client
	client := newOptimizedAPIClient(appKey)
	startDate := time.Date(*year, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(*year, 12, 31, 0, 0, 0, 0, time.UTC)

	fmt.Printf("Fetching daily data for station %d, year %d...\n", *stationID, *year)
	records, fetchMetrics, err := client.FetchDailyDataStreaming(*stationID, api.FormatCIMISDate(startDate), api.FormatCIMISDate(endDate))
	if err != nil {
		return fmt.Errorf("failed to fetch data: %w", err)
	}

	if fetchMetrics != nil {
		fmt.Printf("  Fetch: %v (DNS: %v, TCP: %v, TLS: %v, TTFB: %v)\n",
			fetchMetrics.TotalDuration, fetchMetrics.DNSLookup, fetchMetrics.TCPConnect,
			fetchMetrics.TLSHandshake, fetchMetrics.TTFB)
	}

	if len(records) == 0 {
		fmt.Printf("No records found for station %d, year %d. The station may not have data for this period.\n", *stationID, *year)
		return nil
	}

	// Write chunk
	chunkInfo, err := writer.WriteDailyChunk(uint16(*stationID), *year, records)
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
