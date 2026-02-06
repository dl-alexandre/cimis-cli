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
	fmt.Printf("Stations:           %d\n", stats["station_count"])
	fmt.Printf("Active stations:    %d\n", stats["active_station_count"])
	fmt.Printf("Total chunks:      %d\n", stats["chunk_count"])
	fmt.Printf("Total rows:        %d\n", stats["total_rows"])
	fmt.Printf("Compressed size:   %.2f MB\n", float64(stats["total_compressed_bytes"].(int64))/(1024*1024))
	fmt.Printf("Avg compression:   %.2fx\n", stats["avg_compression_ratio"])
}
