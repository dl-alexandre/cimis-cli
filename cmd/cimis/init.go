package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/dl-alexandre/cimis-tsdb/metadata"
)

func cmdInit(dataDir string) {
	// Create directories
	dirs := []string{
		filepath.Join(dataDir, "stations"),
		filepath.Join(dataDir, "spatial"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatalf("Failed to create directory %s: %v", dir, err)
		}
	}

	// Initialize metadata database
	dbPath := filepath.Join(dataDir, "metadata.sqlite3")
	store, err := metadata.NewStore(dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize metadata database: %v", err)
	}
	defer store.Close()

	fmt.Println("Database initialized successfully")
	fmt.Printf("Data directory: %s\n", dataDir)
	fmt.Printf("Metadata: %s\n", dbPath)
}
