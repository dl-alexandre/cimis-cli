package main

import (
	"fmt"
	"path/filepath"

	"github.com/dl-alexandre/cimis-tsdb/metadata"
)

func cmdStats(dataDir string) {
	fatalIfErr(runStats(dataDir))
}

func runStats(dataDir string) error {
	dbPath := filepath.Join(dataDir, "metadata.sqlite3")
	store, err := metadata.NewStore(dbPath)
	if err != nil {
		return fmt.Errorf("failed to open metadata store: %w", err)
	}
	defer store.Close()

	stats, err := getDatabaseStats(store)
	if err != nil {
		return fmt.Errorf("failed to get stats: %w", err)
	}

	fmt.Println("Database Statistics")
	fmt.Println("===================")
	fmt.Printf("Stations:           %d\n", stats.StationCount)
	fmt.Printf("Active stations:    %d\n", stats.ActiveStationCount)
	fmt.Printf("Total chunks:       %d\n", stats.ChunkCount)
	fmt.Printf("Total rows:         %d\n", stats.TotalRows)
	fmt.Printf("Compressed size:    %.2f MB\n", float64(stats.TotalCompressedBytes)/(1024*1024))
	fmt.Printf("Avg compression:    %.2fx\n", stats.AvgCompressionRatio)
	return nil
}
