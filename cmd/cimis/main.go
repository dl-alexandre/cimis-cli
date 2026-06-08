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

var (
	autoUpdateCheck = cli.AutoUpdateCheck
	checkForUpdates = cli.CheckForUpdates
	exitProcess     = os.Exit
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
	if code := run(os.Args); code != 0 {
		exitProcess(code)
	}
}

func commandExitCode(err error) int {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	return 0
}

func run(args []string) int {
	// Binary name is always cimis (set this BEFORE any version calls)
	cliver.BinaryName = "cimis"

	// Start automatic update check in background (non-blocking)
	autoUpdateCheck()

	if len(args) < 2 {
		printUsage()
		return 1
	}

	// Global flags
	fs := flag.NewFlagSet("cimis", flag.ContinueOnError)
	dataDir := fs.String("data-dir", "./data", "Data directory path")
	appKey := fs.String("app-key", os.Getenv("CIMIS_APP_KEY"), "CIMIS API app key")

	// Subcommands
	switch args[1] {
	case "version":
		fmt.Printf("cimis %s (%s) built %s\n", cliver.Version, cliver.GitCommit, cliver.BuildTime)

	case "check-updates":
		force := false
		format := "table"
		for i := 2; i < len(args); i++ {
			if args[i] == "-force" || args[i] == "--force" {
				force = true
			} else if args[i] == "-json" || args[i] == "--json" {
				format = "json"
			}
		}
		_ = force
		_ = format
		if err := checkForUpdates(force, format); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return 1
		}

	case "init":
		return commandExitCode(runInit(*dataDir))

	case "fetch":
		fmt.Fprintln(os.Stderr, "Warning: 'fetch' command is deprecated. Use 'fetch-streaming' for better performance.")
		return commandExitCode(runFetch(*dataDir, *appKey, args[2:]))

	case "fetch-streaming":
		return commandExitCode(runFetchStreaming(*dataDir, *appKey, args[2:]))

	case "ingest":
		return commandExitCode(runIngest(*dataDir, *appKey, args[2:]))

	case "ingest-opt":
		return commandExitCode(runIngestOptimized(*dataDir, *appKey, args[2:]))

	case "query":
		return commandExitCode(runQuery(*dataDir, args[2:]))

	case "stats":
		return commandExitCode(runStats(*dataDir))

	case "verify":
		return commandExitCode(runVerify(*dataDir))

	case "profile":
		return commandExitCode(runProfile(*dataDir, args[2:]))

	case "register":
		cmdRegister()

	case "login":
		cmdLogin()

	case "api-docs":
		cmdAPI()

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", args[1])
		printUsage()
		return 1
	}

	return 0
}

func printUsage() {
	fmt.Println("Usage: cimis <command> [options]")
}
