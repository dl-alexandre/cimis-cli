// Main entry point for the cimis CLI.
package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

var (
	// Version is set during build
	Version = "dev"
	// GitCommit is set during build
	GitCommit = "unknown"
	// BuildTime is set during build
	BuildTime = "unknown"
)

// parseCacheSize parses cache size strings like "100MB", "1GB" to bytes.
// Returns the size in bytes, or 0 if parsing fails.
func parseCacheSize(sizeStr string) int64 {
	if sizeStr == "" {
		return 0
	}

	sizeStr = strings.TrimSpace(sizeStr)
	sizeStr = strings.ToUpper(sizeStr)

	// Try to parse with suffix
	if strings.HasSuffix(sizeStr, "GB") {
		numStr := strings.TrimSuffix(sizeStr, "GB")
		if num, err := strconv.ParseFloat(numStr, 64); err == nil {
			return int64(num * 1024 * 1024 * 1024)
		}
	} else if strings.HasSuffix(sizeStr, "MB") {
		numStr := strings.TrimSuffix(sizeStr, "MB")
		if num, err := strconv.ParseFloat(numStr, 64); err == nil {
			return int64(num * 1024 * 1024)
		}
	} else if strings.HasSuffix(sizeStr, "KB") {
		numStr := strings.TrimSuffix(sizeStr, "KB")
		if num, err := strconv.ParseFloat(numStr, 64); err == nil {
			return int64(num * 1024)
		}
	} else {
		// Try to parse as plain bytes
		if num, err := strconv.ParseInt(sizeStr, 10, 64); err == nil {
			return num
		}
	}

	return 0
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	// Global flags
	dataDir := flag.String("data-dir", "./data", "Data directory path")
	appKey := flag.String("app-key", os.Getenv("CIMIS_APP_KEY"), "CIMIS API app key")

	// Subcommands
	switch os.Args[1] {
	case "version":
		fmt.Printf("cimis %s (%s) built %s\n", Version, GitCommit, BuildTime)

	case "init":
		cmdInit(*dataDir)

	case "fetch":
		fmt.Fprintln(os.Stderr, "Warning: 'fetch' command is deprecated. Use 'fetch-streaming' for better performance.")
		cmdFetch(*dataDir, *appKey, os.Args[2:])

	case "fetch-streaming":
		cmdFetchStreaming(*dataDir, *appKey, os.Args[2:])

	case "ingest":
		cmdIngest(*dataDir, *appKey, os.Args[2:])

	case "ingest-opt":
		cmdIngestOptimized(*dataDir, *appKey, os.Args[2:])

	case "query":
		cmdQuery(*dataDir, os.Args[2:])

	case "stats":
		cmdStats(*dataDir)

	case "verify":
		cmdVerify(*dataDir)

	case "profile":
		cmdProfile(*dataDir, os.Args[2:])

	case "register":
		cmdRegister()

	case "login":
		cmdLogin()

	case "api-docs":
		cmdAPI()

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`Usage: cimis <command> [options]

Commands:
  version          Show version information
  init             Initialize database directories and metadata
  fetch            Fetch data from CIMIS API (DEPRECATED: use fetch-streaming)
  fetch-streaming  Fetch with optimized streaming + detailed metrics
  ingest           Fetch and store using streaming (production default)
  query            Query stored data
  stats            Show database statistics
  verify           Verify chunk integrity
  profile          CPU, memory, and performance profiling
  register         Open CIMIS registration page in browser
  login            Open CIMIS login page in browser
  api-docs         Open CIMIS API documentation in browser

Global Options:
  -data-dir string    Data directory (default: ./data)
  -app-key string     CIMIS API app key (or CIMIS_APP_KEY env var)

Examples:
   # Initialize database
   cimis init

   # Fetch recent data for station 2
   cimis fetch -station 2 -days 30

   # Fetch multiple stations with streaming and detailed metrics
   cimis fetch-streaming -stations 2,5,10 -year 2024 -concurrency 8 -perf

   # Ingest data for a specific year
   cimis ingest -station 2 -year 2020

   # Query June 2020 data
   cimis query -station 2 -start 2020-06-01 -end 2020-06-30

    # Query with caching and performance metrics
    cimis query -station 2 -start 2020-06-01 -end 2020-06-30 -cache 100MB -perf

    # Open CIMIS registration page to get API key
    cimis register

    # Open CIMIS login page
    cimis login

    # Open CIMIS API documentation
    cimis api-docs`)
}
