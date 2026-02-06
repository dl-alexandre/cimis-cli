package main

import (
	"fmt"
	"log"
	"path/filepath"

	"github.com/dl-alexandre/cimis-tsdb/metadata"
)

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
	fmt.Printf("Stations:           %d\n", stats.StationCount)
	fmt.Printf("Active stations:    %d\n", stats.ActiveStationCount)
	fmt.Printf("Total chunks:      %d\n", stats.ChunkCount)
	fmt.Printf("Total rows:        %d\n", stats.TotalRows)
	fmt.Printf("Compressed size:   %.2f MB\n", float64(stats.TotalCompressedBytes)/(1024*1024))
	fmt.Printf("Avg compression:   %.2fx\n", stats.AvgCompressionRatio)
}
