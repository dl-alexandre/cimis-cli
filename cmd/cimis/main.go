package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/dl-alexandre/cimis-cli/internal/cli"
	cliver "github.com/dl-alexandre/cli-tools/version"
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
	// Binary name is always cimis (set this BEFORE any version calls)
	cliver.BinaryName = "cimis"

	// Start automatic update check in background (non-blocking)
	cli.AutoUpdateCheck()

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
		fmt.Printf("cimis %s (%s) built %s\n", cliver.Version, cliver.GitCommit, cliver.BuildTime)

	case "check-updates":
		force := false
		format := "table"
		for i := 2; i < len(os.Args); i++ {
			if os.Args[i] == "-force" || os.Args[i] == "--force" {
				force = true
			} else if os.Args[i] == "-json" || os.Args[i] == "--json" {
				format = "json"
			}
		}
		_ = force
		_ = format
		if err := cli.CheckForUpdates(force, format); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

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
	fmt.Println("Usage: cimis <command> [options]")
}
