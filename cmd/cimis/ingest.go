package main

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"time"

	"github.com/dl-alexandre/cimis-cli/internal/api"
	"github.com/dl-alexandre/cimis-tsdb/metadata"
	"github.com/dl-alexandre/cimis-tsdb/storage"
)

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
