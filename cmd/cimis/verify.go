package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/dl-alexandre/cimis-tsdb/storage"
)

var (
	verifyReadDir  = os.ReadDir
	verifyReadFile = os.ReadFile
)

func cmdVerify(dataDir string) {
	fatalIfErr(runVerify(dataDir))
}

func runVerify(dataDir string) error {
	// Walk data directory
	stationsDir := filepath.Join(dataDir, "stations")
	entries, err := verifyReadDir(stationsDir)
	if err != nil {
		return fmt.Errorf("failed to read stations directory: %w", err)
	}

	var verified, failed int

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		stationDir := filepath.Join(stationsDir, entry.Name())
		chunks, err := verifyReadDir(stationDir)
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
			compressed, err := verifyReadFile(filePath)
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
		return fmt.Errorf("%d chunk(s) failed verification", failed)
	}
	return nil
}
