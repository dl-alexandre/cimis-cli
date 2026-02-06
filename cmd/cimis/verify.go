package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"

	"github.com/dl-alexandre/cimis-tsdb/storage"
)

func cmdVerify(dataDir string) {
	// Walk data directory
	stationsDir := filepath.Join(dataDir, "stations")
	entries, err := os.ReadDir(stationsDir)
	if err != nil {
		log.Fatalf("Failed to read stations directory: %v", err)
	}

	var verified, failed int

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		stationDir := filepath.Join(stationsDir, entry.Name())
		chunks, err := os.ReadDir(stationDir)
		if err != nil {
			continue
		}

		// Parse station ID from directory name
		stationID, _ := strconv.Atoi(entry.Name())

		for _, chunk := range chunks {
			if chunk.IsDir() || filepath.Ext(chunk.Name()) != ".zst" {
				continue
			}

			// Try to read and decompress
			filePath := filepath.Join(stationDir, chunk.Name())
			compressed, err := os.ReadFile(filePath)
			if err != nil {
				fmt.Printf("FAIL: %s - read error: %v\n", filePath, err)
				failed++
				continue
			}

			_, err = storage.Decompress(nil, compressed)
			if err != nil {
				fmt.Printf("FAIL: %s - decompress error: %v\n", filePath, err)
				failed++
				continue
			}

			fmt.Printf("OK: %s (station %d)\n", filePath, stationID)
			verified++
		}
	}

	fmt.Printf("\nVerification complete: %d OK, %d failed\n", verified, failed)
	if failed > 0 {
		os.Exit(1)
	}
}
