package main

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"time"

	"github.com/dl-alexandre/cimis-tsdb/metadata"
	"github.com/dl-alexandre/cimis-tsdb/storage"
	"github.com/dl-alexandre/cimis-tsdb/types"
)

func cmdQuery(dataDir string, args []string) {
	// Parse flags
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	stationID := fs.Int("station", 0, "Station ID")
	startDate := fs.String("start", "", "Start date (YYYY-MM-DD)")
	endDate := fs.String("end", "", "End date (YYYY-MM-DD)")
	hourly := fs.Bool("hourly", false, "Query hourly data (default: daily)")
	perf := fs.Bool("perf", false, "Show performance metrics")
	cache := fs.String("cache", "", "Enable caching with specified size (e.g., 100MB, 1GB)")

	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}

	if *stationID == 0 {
		log.Fatal("Station ID required")
	}

	// Start total query timer
	queryStart := time.Now()

	// Parse dates
	start, err := time.Parse("2006-01-02", *startDate)
	if err != nil {
		log.Fatalf("Invalid start date: %v", err)
	}
	end, err := time.Parse("2006-01-02", *endDate)
	if err != nil {
		log.Fatalf("Invalid end date: %v", err)
	}

	// Initialize metadata store
	dbPath := filepath.Join(dataDir, "metadata.sqlite3")
	store, err := metadata.NewStore(dbPath)
	if err != nil {
		log.Fatalf("Failed to open metadata store: %v", err)
	}
	defer store.Close()

	// Initialize chunk reader (with caching if requested)
	var reader interface {
		ReadDailyChunk(stationID uint16, year int) ([]types.DailyRecord, error)
		ReadHourlyChunk(stationID uint16, year int) ([]types.HourlyRecord, error)
	}
	var cachedReader *storage.CachedChunkReader

	if *cache != "" {
		cacheSize := parseCacheSize(*cache)
		if cacheSize <= 0 {
			log.Fatalf("Invalid cache size: %s", *cache)
		}
		cachedReader = storage.NewCachedChunkReader(dataDir, cacheSize)
		reader = cachedReader
	} else {
		reader = storage.NewChunkReader(dataDir)
	}

	// Get chunks in range
	startYear := start.Year()
	endYear := end.Year()
	dataType := types.DataTypeDaily
	if *hourly {
		dataType = types.DataTypeHourly
	}

	// Time metadata lookup
	metadataStart := time.Now()
	chunks, err := store.GetChunksForYearRange(uint16(*stationID), startYear, endYear, dataType)
	metadataDuration := time.Since(metadataStart)
	if err != nil {
		log.Fatalf("Failed to get chunks: %v", err)
	}

	if len(chunks) == 0 {
		fmt.Printf("No data found for station %d in range %s to %s\n", *stationID, *startDate, *endDate)
		return
	}

	// Read and filter records
	fmt.Printf("Querying %d chunks...\n", len(chunks))

	var totalRecords int
	var chunksRead int
	var totalChunkReadTime time.Duration
	var totalFilterTime time.Duration

	for _, chunk := range chunks {
		if *hourly {
			// Time chunk read
			chunkReadStart := time.Now()
			records, err := reader.ReadHourlyChunk(chunk.StationID, chunk.Year)
			chunkReadDuration := time.Since(chunkReadStart)
			totalChunkReadTime += chunkReadDuration
			chunksRead++

			if err != nil {
				log.Printf("Warning: failed to read chunk %d: %v", chunk.Year, err)
				continue
			}
			// Filter by timestamp range
			filterStart := time.Now()
			startTs := uint32(start.Sub(time.Date(1985, 1, 1, 0, 0, 0, 0, time.UTC)).Hours())
			endTs := uint32(end.Sub(time.Date(1985, 1, 1, 0, 0, 0, 0, time.UTC)).Hours())

			for _, r := range records {
				if r.Timestamp >= startTs && r.Timestamp < endTs {
					totalRecords++
					if totalRecords <= 10 {
						ts := time.Date(1985, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(r.Timestamp) * time.Hour)
						fmt.Printf("  %s: Temp=%.1f°C ET=%.2fmm Wind=%.1fm/s Humidity=%d%%\n",
							ts.Format("2006-01-02 15:00"),
							float64(r.Temperature)/10.0,
							float64(r.ET)/1000.0,
							float64(r.WindSpeed)/10.0,
							r.Humidity)
					}
				}
			}
			totalFilterTime += time.Since(filterStart)
		} else {
			// Time chunk read
			chunkReadStart := time.Now()
			records, err := reader.ReadDailyChunk(chunk.StationID, chunk.Year)
			chunkReadDuration := time.Since(chunkReadStart)
			totalChunkReadTime += chunkReadDuration
			chunksRead++

			if err != nil {
				log.Printf("Warning: failed to read chunk %d: %v", chunk.Year, err)
				continue
			}
			// Filter by timestamp range
			filterStart := time.Now()
			startTs := uint32(start.Sub(time.Date(1985, 1, 1, 0, 0, 0, 0, time.UTC)).Hours() / 24)
			endTs := uint32(end.Sub(time.Date(1985, 1, 1, 0, 0, 0, 0, time.UTC)).Hours() / 24)

			for _, r := range records {
				if r.Timestamp >= startTs && r.Timestamp < endTs {
					totalRecords++
					if totalRecords <= 10 {
						ts := time.Date(1985, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(r.Timestamp) * 24 * time.Hour)
						fmt.Printf("  %s: Temp=%.1f°C ET=%.2fmm Wind=%.1fm/s Humidity=%d%%\n",
							ts.Format("2006-01-02"),
							float64(r.Temperature)/10.0,
							float64(r.ET)/100.0,
							float64(r.WindSpeed)/10.0,
							r.Humidity)
					}
				}
			}
			totalFilterTime += time.Since(filterStart)
		}
	}

	fmt.Printf("\nTotal records: %d\n", totalRecords)
	if totalRecords > 10 {
		fmt.Printf("(showing first 10)\n")
	}

	// Print performance metrics if requested
	if *perf {
		totalDuration := time.Since(queryStart)
		avgChunkReadTime := time.Duration(0)
		if chunksRead > 0 {
			avgChunkReadTime = totalChunkReadTime / time.Duration(chunksRead)
		}
		avgRecordTime := time.Duration(0)
		if totalRecords > 0 {
			avgRecordTime = totalFilterTime / time.Duration(totalRecords)
		}
		recordsPerSec := float64(0)
		if totalDuration.Seconds() > 0 {
			recordsPerSec = float64(totalRecords) / totalDuration.Seconds()
		}

		fmt.Println("\n=== Performance Metrics ===")
		fmt.Printf("Total query duration:      %v\n", totalDuration)
		fmt.Printf("Metadata lookup time:      %v\n", metadataDuration)
		fmt.Printf("Chunks read:               %d\n", chunksRead)
		fmt.Printf("Average chunk read time:   %v\n", avgChunkReadTime)
		fmt.Printf("Total filter/process time: %v\n", totalFilterTime)
		fmt.Printf("Average record time:       %v\n", avgRecordTime)
		fmt.Printf("Records per second:        %.2f\n", recordsPerSec)

		// Print cache statistics if caching was enabled
		if cachedReader != nil {
			cacheStats := cachedReader.GetCacheStats()
			fmt.Println("\n=== Cache Statistics ===")
			fmt.Println(storage.FormatCacheStats(cacheStats))
		}
	}
}
