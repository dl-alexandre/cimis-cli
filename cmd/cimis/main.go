package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/alecthomas/kong"
	"github.com/dl-alexandre/cimis-cli/internal/cli"
	cliver "github.com/dl-alexandre/cli-tools/version"
)

var (
	autoUpdateCheck = cli.AutoUpdateCheck
	checkForUpdates = cli.CheckForUpdates
	exitProcess     = os.Exit
)

// Globals are options accepted by every cimis command.
type Globals struct {
	DataDir string `help:"Data directory path" name:"data-dir" default:"./data"`
	AppKey  string `help:"CIMIS API app key" name:"app-key" env:"CIMIS_APP_KEY"`
}

// CLI is the Kong command tree for cimis.
type CLI struct {
	Globals

	Version        VersionCmd        `cmd:"" help:"Show version information"`
	CheckUpdates   CheckUpdatesCmd   `cmd:"" help:"Check for available updates"`
	Init           InitCmd           `cmd:"" help:"Initialize a local CIMIS database"`
	Fetch          FetchCmd          `cmd:"" help:"Fetch recent data for one station"`
	FetchStreaming FetchStreamingCmd `cmd:"" help:"Fetch and store data using the streaming API"`
	Ingest         IngestCmd         `cmd:"" help:"Ingest daily data into the local database"`
	IngestOpt      IngestOptCmd      `cmd:"" help:"Ingest daily data using optimized encoding"`
	Query          QueryCmd          `cmd:"" help:"Query locally stored data"`
	Stats          StatsCmd          `cmd:"" help:"Show database statistics"`
	Verify         VerifyCmd         `cmd:"" help:"Verify database contents"`
	Profile        ProfileCmd        `cmd:"" help:"Profile CIMIS CLI operations"`
	Register       RegisterCmd       `cmd:"" help:"Open the CIMIS registration page"`
	Login          LoginCmd          `cmd:"" help:"Open the CIMIS login page"`
	APIDocs        APIDocsCmd        `cmd:"" help:"Open CIMIS API documentation"`
}

// parseCLI constructs a fresh parser so callers and tests can control output.

func parseCLI(root *CLI, args []string, stdout, stderr io.Writer, exit func(int)) (*kong.Context, error) {
	parser, err := kong.New(root,
		kong.Name("cimis"),
		kong.Description("Command-line tools for the California Irrigation Management Information System (CIMIS)."),
		kong.Writers(stdout, stderr),
		kong.Exit(exit),
		kong.ConfigureHelp(kong.HelpOptions{Compact: true}),
	)
	if err != nil {
		return nil, err
	}
	ctx, err := parser.Parse(normalizeLegacyFlags(args))
	return ctx, err
}

// normalizeLegacyFlags keeps accepting the standard-library form of long flags
// (for example, -station) while Kong handles parsing and validation.
func normalizeLegacyFlags(args []string) []string {
	legacyNames := map[string]struct{}{
		"help": {}, "data-dir": {}, "app-key": {}, "force": {}, "json": {}, "station": {}, "days": {}, "hourly": {},
		"stations": {}, "year": {}, "start": {}, "end": {}, "concurrency": {}, "gzip": {}, "format": {}, "dry-run": {},
		"perf": {}, "allocs": {}, "retries": {}, "out": {}, "compression": {}, "cache": {}, "cpu": {}, "heap": {},
		"goroutines": {}, "mutex": {}, "duration": {}, "server": {}, "stats": {},
	}
	normalized := make([]string, len(args))
	for i, arg := range args {
		if len(arg) > 2 && strings.HasPrefix(arg, "-") && !strings.HasPrefix(arg, "--") {
			name := strings.TrimPrefix(arg[1:], "-")
			name, _, _ = strings.Cut(name, "=")
			if _, ok := legacyNames[name]; ok {
				arg = "-" + arg
			}
		}
		normalized[i] = arg
	}
	return normalized
}

func main() {
	if code := run(os.Args); code != 0 {
		exitProcess(code)
	}
}

func run(args []string) int {
	// Binary name is set before either version output or update checking.
	cliver.BinaryName = "cimis"
	autoUpdateCheck()

	if len(args) < 2 {
		printUsage()
		return 1
	}

	root := &CLI{}
	exitCode := -1
	ctx, err := parseCLI(root, args[1:], os.Stdout, os.Stderr, func(code int) {
		exitCode = code
	})
	if exitCode >= 0 {
		return exitCode
	}
	if err != nil {
		if len(args) > 1 && !knownCommand(args[1]) {
			fmt.Fprintf(os.Stderr, "Unknown command: %s\n", args[1])
			printUsage()
			return 1
		}
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		var parseErr *kong.ParseError
		if errors.As(err, &parseErr) && parseErr.Context != nil {
			_ = parseErr.Context.PrintUsage(false)
		}
		return 1
	}
	if err := ctx.Run(&root.Globals); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	return 0
}

func knownCommand(name string) bool {
	switch name {
	case "version", "check-updates", "init", "fetch", "fetch-streaming", "ingest", "ingest-opt", "query", "stats", "verify", "profile", "register", "login", "api-docs":
		return true
	default:
		return false
	}
}

func printUsage() {
	fmt.Println("Usage: cimis <command> [options]")
}

// parseCacheSize parses cache size strings like "100MB" and "1GB" to bytes.
// Returns the size in bytes, or 0 if parsing fails.
func parseCacheSize(sizeStr string) int64 {
	if sizeStr == "" {
		return 0
	}

	sizeStr = strings.ToUpper(strings.TrimSpace(sizeStr))
	for _, suffix := range []struct {
		name       string
		multiplier float64
	}{{"GB", 1024 * 1024 * 1024}, {"MB", 1024 * 1024}, {"KB", 1024}} {
		if strings.HasSuffix(sizeStr, suffix.name) {
			num, err := strconv.ParseFloat(strings.TrimSuffix(sizeStr, suffix.name), 64)
			if err != nil {
				return 0
			}
			return int64(num * suffix.multiplier)
		}
	}
	value, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return 0
	}
	return value
}
