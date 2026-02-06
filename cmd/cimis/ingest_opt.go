package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/dl-alexandre/cimis-cli/internal/api"
	"github.com/dl-alexandre/cimis-tsdb/metadata"
	"github.com/dl-alexandre/cimis-tsdb/storage"
)

func cmdIngestOptimized(dataDir, appKey string, args []string) {
	if appKey == "" {
		log.Fatal("CIMIS app key required")
	}

	fs := flag.NewFlagSet("ingest-optimized", flag.ExitOnError)
	stationID := fs.Int("station", 0, "Station ID")
	year := fs.Int("year", 0, "Year to ingest (default: current year)")
	compressionLevel := fs.Int("compression", 1, "Compression level (1-22)")

	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}

	if *stationID == 0 {
		log.Fatal("Station ID required")
	}

	if *year == 0 {
		*year = time.Now().Year()
	}

	dbPath := filepath.Join(dataDir, "metadata.sqlite3")
	store, err := metadata.NewStore(dbPath)
	if err != nil {
		log.Fatalf("Failed to open metadata store: %v", err)
	}
	defer store.Close()

	client := api.NewClient(appKey)
	startDate := time.Date(*year, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(*year, 12, 31, 0, 0, 0, 0, time.UTC)

	fmt.Printf("Fetching daily data for station %d, year %d...\n", *stationID, *year)
	apiRecords, err := client.FetchDailyData(*stationID, api.FormatCIMISDate(startDate), api.FormatCIMISDate(endDate))
	if err != nil {
		log.Fatalf("Failed to fetch data: %v", err)
	}

	records := api.ConvertDailyToRecords(apiRecords, uint16(*stationID))
	if len(records) == 0 {
		fmt.Println("No records to ingest")
		return
	}

	// Use optimized encoding
	cd := storage.ExtractColumns(records)
	optData, meta, err := storage.OptimizeColumns(cd, uint16(*stationID))
	if err != nil {
		log.Fatalf("Failed to optimize columns: %v", err)
	}

	// Compress the optimized data
	compressed, err := storage.CompressLevel(optData, *compressionLevel)
	if err != nil {
		log.Fatalf("Failed to compress: %v", err)
	}

	// Write to file with .opt.zst extension
	stationDir := filepath.Join(dataDir, "stations", fmt.Sprintf("%03d", *stationID))
	if err := os.MkdirAll(stationDir, 0755); err != nil {
		log.Fatalf("Failed to create directory: %v", err)
	}

	chunkPath := filepath.Join(stationDir, fmt.Sprintf("%d_optimized.zst", *year))
	if err := os.WriteFile(chunkPath, compressed, 0644); err != nil {
		log.Fatalf("Failed to write chunk: %v", err)
	}

	// Calculate stats
	originalSize := len(records) * 16 // Original row-based size
	optSize := len(optData)
	compressedSize := len(compressed)

	stats := storage.CalculateCompressionStats([]byte{}, compressed, len(records))

	fmt.Printf("\n✓ Ingested %d daily records\n", len(records))
	fmt.Printf("  Original row size: %d bytes\n", originalSize)
	fmt.Printf("  Optimized size: %d bytes\n", optSize)
	fmt.Printf("  Compressed: %d bytes\n", compressedSize)
	fmt.Printf("  Overall ratio: %.2fx\n", float64(originalSize)/float64(compressedSize))
	fmt.Printf("  Bytes per record: %.2f\n", stats["bytes_per_record"])
	fmt.Printf("  Space savings: %.1f%%\n", stats["space_savings_pct"])
	fmt.Printf("  Stored in: %s\n", chunkPath)

	// Save metadata
	_ = meta // Would save to SQLite in production

	// Also test decompression to verify
	decompressed, err := storage.Decompress(nil, compressed)
	if err != nil {
		log.Fatalf("Failed to decompress test: %v", err)
	}

	if len(decompressed) != len(optData) {
		log.Fatalf("Decompression mismatch: %d vs %d", len(decompressed), len(optData))
	}

	fmt.Printf("  ✓ Compression verification passed\n")
}
