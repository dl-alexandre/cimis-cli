package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/dl-alexandre/cimis-tsdb/metadata"
)

func cmdInit(dataDir string) {
	fatalIfErr(runInit(dataDir))
}

func runInit(dataDir string) error {
	// Create directories
	dirs := []string{
		filepath.Join(dataDir, "stations"),
		filepath.Join(dataDir, "spatial"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	// Initialize metadata database
	dbPath := filepath.Join(dataDir, "metadata.sqlite3")
	store, err := metadata.NewStore(dbPath)
	if err != nil {
		return fmt.Errorf("failed to initialize metadata database: %w", err)
	}
	defer store.Close()

	fmt.Println("Database initialized successfully")
	fmt.Printf("Data directory: %s\n", dataDir)
	fmt.Printf("Metadata: %s\n", dbPath)
	return nil
}
