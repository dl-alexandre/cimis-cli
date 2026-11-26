package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/dl-alexandre/cimis-cli/internal/api"
	"github.com/dl-alexandre/cimis-tsdb/metadata"
	"github.com/dl-alexandre/cimis-tsdb/storage"
)

type IngestOptCmd struct {
	Station     int `help:"Station ID" name:"station"`
	Year        int `help:"Year to ingest (defaults to current year)" name:"year"`
	Compression int `help:"Compression level (1-22)" name:"compression" default:"1"`
}

var (
	optimizeColumns = storage.OptimizeColumns
	compressLevel   = storage.CompressLevel
	decompressData  = storage.Decompress
)

func cmdIngestOptimized(dataDir, appKey string, args []string) {
	fatalIfErr(runIngestOptimized(dataDir, appKey, args))
}

func runIngestOptimized(dataDir, appKey string, args []string) error {
	var options IngestOptCmd
	if err := parseCommandOptions("ingest-opt", args, &options); err != nil {
		return err
	}
	return runIngestOptimizedOptions(dataDir, appKey, options)
}

func (c *IngestOptCmd) Run(globals *Globals) error {
	return runIngestOptimizedOptions(globals.DataDir, globals.AppKey, *c)
}

func runIngestOptimizedOptions(dataDir, appKey string, options IngestOptCmd) error {
	if appKey == "" {
		return fmt.Errorf("CIMIS app key required")
	}
	if options.Station == 0 {
		return fmt.Errorf("station ID required")
	}

	if options.Year == 0 {
		options.Year = time.Now().Year()
	}

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	dbPath := filepath.Join(dataDir, "metadata.sqlite3")
	store, err := metadata.NewStore(dbPath)
	if err != nil {
		return fmt.Errorf("failed to open metadata store: %w", err)
	}
	defer store.Close()

	client := newAPIClient(appKey)
	startDate := time.Date(options.Year, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(options.Year, 12, 31, 0, 0, 0, 0, time.UTC)

	fmt.Printf("Fetching daily data for station %d, year %d...\n", options.Station, options.Year)
	apiRecords, err := client.FetchDailyData(options.Station, api.FormatCIMISDate(startDate), api.FormatCIMISDate(endDate))
	if err != nil {
		return fmt.Errorf("failed to fetch data: %w", err)
	}

	records := api.ConvertDailyToRecords(apiRecords, uint16(options.Station))
	if len(records) == 0 {
		fmt.Println("No records to ingest")
		return nil
	}

	// Use optimized encoding
	cd := storage.ExtractColumns(records)
	optData, meta, err := optimizeColumns(cd, uint16(options.Station))
	if err != nil {
		return fmt.Errorf("failed to optimize columns: %w", err)
	}

	// Compress the optimized data
	compressed, err := compressLevel(optData, options.Compression)
	if err != nil {
		return fmt.Errorf("failed to compress: %w", err)
	}

	// Write to file with .opt.zst extension
	stationDir := filepath.Join(dataDir, "stations", fmt.Sprintf("%03d", options.Station))
	if err := os.MkdirAll(stationDir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	chunkPath := filepath.Join(stationDir, fmt.Sprintf("%d_optimized.zst", options.Year))
	if err := os.WriteFile(chunkPath, compressed, 0644); err != nil {
		return fmt.Errorf("failed to write chunk: %w", err)
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
	fmt.Printf("  Bytes per record: %.2f\n", stats.BytesPerRecord)
	fmt.Printf("  Space savings: %.1f%%\n", stats.SpaceSavingsPct)
	fmt.Printf("  Compression ratio: %.2fx\n", stats.CompressionRatio)
	fmt.Printf("  Stored in: %s\n", chunkPath)

	// Save metadata
	_ = meta // Would save to SQLite in production

	// Also test decompression to verify
	decompressed, err := decompressData(nil, compressed)
	if err != nil {
		return fmt.Errorf("failed to decompress test: %w", err)
	}

	if len(decompressed) != len(optData) {
		return fmt.Errorf("decompression mismatch: %d vs %d", len(decompressed), len(optData))
	}

	fmt.Printf("  ✓ Compression verification passed\n")
	return nil
}
