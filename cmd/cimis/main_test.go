package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dl-alexandre/cimis-cli/internal/api"
	profilepkg "github.com/dl-alexandre/cimis-cli/internal/profile"
	"github.com/dl-alexandre/cimis-tsdb/metadata"
	"github.com/dl-alexandre/cimis-tsdb/storage"
	"github.com/dl-alexandre/cimis-tsdb/types"
)

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	original := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() error = %v", err)
	}
	os.Stdout = w
	closed := false
	defer func() {
		os.Stdout = original
		if !closed {
			_ = w.Close()
		}
	}()

	fn()

	if err := w.Close(); err != nil {
		t.Fatalf("close stdout pipe writer: %v", err)
	}
	closed = true

	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read stdout pipe: %v", err)
	}
	return string(out)
}

func captureStderr(t *testing.T, fn func()) string {
	t.Helper()

	original := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() error = %v", err)
	}
	os.Stderr = w
	closed := false
	defer func() {
		os.Stderr = original
		if !closed {
			_ = w.Close()
		}
	}()

	fn()

	if err := w.Close(); err != nil {
		t.Fatalf("close stderr pipe writer: %v", err)
	}
	closed = true

	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read stderr pipe: %v", err)
	}
	return string(out)
}

func withNoAutoUpdate(t *testing.T) {
	t.Helper()

	originalAuto := autoUpdateCheck
	originalCheck := checkForUpdates
	t.Cleanup(func() {
		autoUpdateCheck = originalAuto
		checkForUpdates = originalCheck
	})

	autoUpdateCheck = func() {}
}

func withTempWorkingDir(t *testing.T) {
	t.Helper()

	original, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd() error = %v", err)
	}
	dir := t.TempDir()
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("Chdir(%s) error = %v", dir, err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(original); err != nil {
			t.Fatalf("restore cwd: %v", err)
		}
	})
}

func TestParseCacheSize(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		// Gigabytes
		{"1GB", 1 * 1024 * 1024 * 1024},
		{"1.5GB", int64(1.5 * 1024 * 1024 * 1024)},
		{"2gb", 2 * 1024 * 1024 * 1024},   // case insensitive
		{" 1GB ", 1 * 1024 * 1024 * 1024}, // whitespace

		// Megabytes
		{"100MB", 100 * 1024 * 1024},
		{"512MB", 512 * 1024 * 1024},
		{"1.5MB", int64(1.5 * 1024 * 1024)},

		// Kilobytes
		{"512KB", 512 * 1024},
		{"1024KB", 1024 * 1024},

		// Plain bytes
		{"1048576", 1048576},
		{"0", 0},

		// Invalid
		{"", 0},
		{"abc", 0},
		{"MB", 0},                  // no number
		{"10XB", 0},                // unknown suffix
		{"-1MB", -1 * 1024 * 1024}, // negative values pass through ParseFloat
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseCacheSize(tt.input)
			if got != tt.want {
				t.Errorf("parseCacheSize(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseCacheSizeNegative(t *testing.T) {
	// Negative values: ParseFloat succeeds but result may be unexpected
	// This documents current behavior
	got := parseCacheSize("-1MB")
	if got >= 0 {
		// If it returns 0, that's fine (means parsing failed)
		// If negative, that's the current behavior
		t.Logf("parseCacheSize(\"-1MB\") = %d (documenting current behavior)", got)
	}
}

func TestParseStationList(t *testing.T) {
	tests := []struct {
		input   string
		want    []int
		wantErr bool
	}{
		// Single station
		{"2", []int{2}, false},
		{"100", []int{100}, false},

		// CSV list
		{"2,5,10", []int{2, 5, 10}, false},
		{" 2 , 5 , 10 ", []int{2, 5, 10}, false}, // with spaces

		// Range
		{"1-5", []int{1, 2, 3, 4, 5}, false},

		// Mixed
		{"2,5-7,10", []int{2, 5, 6, 7, 10}, false},

		// Errors
		{"abc", nil, true},
		{"1-2-3", nil, true}, // invalid range
		{"1-abc", nil, true}, // invalid range end
		{"abc-5", nil, true}, // invalid range start
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseStationList(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseStationList(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if len(got) != len(tt.want) {
					t.Errorf("parseStationList(%q) = %v, want %v", tt.input, got, tt.want)
					return
				}
				for i := range got {
					if got[i] != tt.want[i] {
						t.Errorf("parseStationList(%q)[%d] = %d, want %d", tt.input, i, got[i], tt.want[i])
					}
				}
			}
		})
	}
}

func TestSortStations(t *testing.T) {
	tests := []struct {
		name  string
		input []int
		want  []int
	}{
		{"already sorted", []int{1, 2, 3}, []int{1, 2, 3}},
		{"reverse", []int{3, 2, 1}, []int{1, 2, 3}},
		{"unsorted", []int{5, 1, 3, 2, 4}, []int{1, 2, 3, 4, 5}},
		{"single", []int{1}, []int{1}},
		{"empty", []int{}, []int{}},
		{"duplicates", []int{3, 1, 3, 2}, []int{1, 2, 3, 3}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := make([]int, len(tt.input))
			copy(input, tt.input)
			sortStations(input)

			if len(input) != len(tt.want) {
				t.Errorf("sortStations(%v) = %v, want %v", tt.input, input, tt.want)
				return
			}
			for i := range input {
				if input[i] != tt.want[i] {
					t.Errorf("sortStations(%v)[%d] = %d, want %d", tt.input, i, input[i], tt.want[i])
				}
			}
		})
	}
}

func TestCmdInitStatsAndVerifyHappyPaths(t *testing.T) {
	dataDir := t.TempDir()

	initOutput := captureStdout(t, func() {
		cmdInit(dataDir)
	})
	if !strings.Contains(initOutput, "Database initialized successfully") {
		t.Fatalf("cmdInit output = %q", initOutput)
	}

	for _, rel := range []string{"stations", "spatial", "metadata.sqlite3"} {
		path := filepath.Join(dataDir, rel)
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected %s to exist: %v", path, err)
		}
	}

	statsOutput := captureStdout(t, func() {
		cmdStats(dataDir)
	})
	if !strings.Contains(statsOutput, "Database Statistics") {
		t.Fatalf("cmdStats output = %q", statsOutput)
	}

	verifyOutput := captureStdout(t, func() {
		cmdVerify(dataDir)
	})
	if !strings.Contains(verifyOutput, "Verification complete: 0 OK, 0 failed") {
		t.Fatalf("cmdVerify output = %q", verifyOutput)
	}
}

func TestRunInitStatsVerifyErrors(t *testing.T) {
	base := t.TempDir()
	filePath := filepath.Join(base, "not-a-dir")
	if err := os.WriteFile(filePath, []byte("file"), 0644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := runInit(filepath.Join(filePath, "child")); err == nil {
		t.Fatal("expected runInit error when parent is a file")
	}

	metadataDirData := t.TempDir()
	if err := os.MkdirAll(filepath.Join(metadataDirData, "metadata.sqlite3"), 0755); err != nil {
		t.Fatalf("MkdirAll metadata dir: %v", err)
	}
	if err := runInit(metadataDirData); err == nil {
		t.Fatal("expected runInit metadata initialization error")
	}

	if err := runStats(filepath.Join(base, "missing")); err == nil {
		t.Fatal("expected runStats error for missing data dir")
	}

	initializedDir := t.TempDir()
	captureStdout(t, func() {
		cmdInit(initializedDir)
	})
	originalGetStats := getDatabaseStats
	t.Cleanup(func() { getDatabaseStats = originalGetStats })
	getDatabaseStats = func(*metadata.Store) (*metadata.DatabaseStats, error) {
		return nil, errors.New("stats failed")
	}
	if err := runStats(initializedDir); err == nil {
		t.Fatal("expected runStats database stats error")
	}

	if err := runVerify(filepath.Join(base, "missing")); err == nil {
		t.Fatal("expected runVerify error for missing stations dir")
	}
}

func TestPrintUsage(t *testing.T) {
	output := captureStdout(t, printUsage)
	if !strings.Contains(output, "Usage: cimis <command> [options]") {
		t.Fatalf("printUsage output = %q", output)
	}
}

func TestRunDispatchVersionAndUsage(t *testing.T) {
	withNoAutoUpdate(t)

	versionOutput := captureStdout(t, func() {
		if code := run([]string{"cimis", "version"}); code != 0 {
			t.Fatalf("version exit code = %d, want 0", code)
		}
	})
	if !strings.Contains(versionOutput, "cimis") {
		t.Fatalf("version output = %q", versionOutput)
	}

	usageOutput := captureStdout(t, func() {
		if code := run([]string{"cimis"}); code != 1 {
			t.Fatalf("usage exit code = %d, want 1", code)
		}
	})
	if !strings.Contains(usageOutput, "Usage: cimis") {
		t.Fatalf("usage output = %q", usageOutput)
	}
}

func TestMainVersionReturnsWithoutExit(t *testing.T) {
	withNoAutoUpdate(t)

	originalArgs := os.Args
	t.Cleanup(func() { os.Args = originalArgs })

	os.Args = []string{"cimis", "version"}
	output := captureStdout(t, main)
	if !strings.Contains(output, "cimis") {
		t.Fatalf("main version output = %q", output)
	}
}

func TestMainExitsOnRunError(t *testing.T) {
	withNoAutoUpdate(t)

	originalArgs := os.Args
	originalExit := exitProcess
	t.Cleanup(func() {
		os.Args = originalArgs
		exitProcess = originalExit
	})

	os.Args = []string{"cimis"}
	exitProcess = func(code int) {
		if code != 1 {
			t.Fatalf("exit code = %d, want 1", code)
		}
		panic("exit")
	}

	expectPanic(t, func() {
		captureStdout(t, main)
	})
}

func TestRunDispatchUnknownAndCheckUpdates(t *testing.T) {
	withNoAutoUpdate(t)

	stderr := captureStderr(t, func() {
		stdout := captureStdout(t, func() {
			if code := run([]string{"cimis", "nope"}); code != 1 {
				t.Fatalf("unknown exit code = %d, want 1", code)
			}
		})
		if !strings.Contains(stdout, "Usage: cimis") {
			t.Fatalf("unknown stdout = %q", stdout)
		}
	})
	if !strings.Contains(stderr, "Unknown command: nope") {
		t.Fatalf("unknown stderr = %q", stderr)
	}

	var gotForce bool
	var gotFormat string
	checkForUpdates = func(force bool, format string) error {
		gotForce = force
		gotFormat = format
		return nil
	}
	if code := run([]string{"cimis", "check-updates", "--force", "--json"}); code != 0 {
		t.Fatalf("check-updates exit code = %d, want 0", code)
	}
	if !gotForce || gotFormat != "json" {
		t.Fatalf("check-updates args force=%v format=%q", gotForce, gotFormat)
	}

	checkForUpdates = func(force bool, format string) error {
		return os.ErrPermission
	}
	stderr = captureStderr(t, func() {
		if code := run([]string{"cimis", "check-updates"}); code != 1 {
			t.Fatalf("check-updates error exit code = %d, want 1", code)
		}
	})
	if !strings.Contains(stderr, "Error:") {
		t.Fatalf("check-updates error stderr = %q", stderr)
	}
}

func TestRunDispatchLocalCommands(t *testing.T) {
	withNoAutoUpdate(t)
	withTempWorkingDir(t)

	if code := run([]string{"cimis", "init"}); code != 0 {
		t.Fatalf("init exit code = %d, want 0", code)
	}
	if code := run([]string{"cimis", "stats"}); code != 0 {
		t.Fatalf("stats exit code = %d, want 0", code)
	}
	if code := run([]string{"cimis", "verify"}); code != 0 {
		t.Fatalf("verify exit code = %d, want 0", code)
	}

	output := captureStdout(t, func() {
		if code := run([]string{"cimis", "profile", "-stats"}); code != 0 {
			t.Fatalf("profile -stats exit code = %d, want 0", code)
		}
	})
	if !strings.Contains(output, "Runtime Statistics") {
		t.Fatalf("profile output = %q", output)
	}
}

func TestRunDispatchAPIBackedCommands(t *testing.T) {
	withNoAutoUpdate(t)
	server := newMockCIMISServer(t)
	defer server.Close()
	installMockCIMISClients(t, server.URL)
	t.Setenv("CIMIS_APP_KEY", "test-key")

	tests := []struct {
		name string
		args []string
	}{
		{"fetch", []string{"cimis", "fetch", "-station", "2", "-days", "1"}},
		{"fetch-streaming", []string{"cimis", "fetch-streaming", "-stations", "2", "-start", "2024-01-01", "-end", "2024-01-31", "-concurrency", "1"}},
		{"ingest", []string{"cimis", "ingest", "-station", "2", "-year", "2024"}},
		{"ingest-opt", []string{"cimis", "ingest-opt", "-station", "2", "-year", "2024"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withTempWorkingDir(t)
			if code := run(tt.args); code != 0 {
				t.Fatalf("%s exit code = %d, want 0", tt.name, code)
			}
		})
	}
}

func TestRunDispatchCommandErrors(t *testing.T) {
	withNoAutoUpdate(t)
	t.Setenv("CIMIS_APP_KEY", "")

	tests := []struct {
		name string
		args []string
	}{
		{"fetch", []string{"cimis", "fetch", "-station", "2"}},
		{"fetch-streaming", []string{"cimis", "fetch-streaming", "-stations", "2"}},
		{"ingest", []string{"cimis", "ingest", "-station", "2"}},
		{"ingest-opt", []string{"cimis", "ingest-opt", "-station", "2"}},
		{"query", []string{"cimis", "query"}},
		{"profile", []string{"cimis", "profile", "-unknown"}},
		{"stats", []string{"cimis", "stats"}},
		{"verify", []string{"cimis", "verify"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withTempWorkingDir(t)
			stderr := captureStderr(t, func() {
				if code := run(tt.args); code != 1 {
					t.Fatalf("%s exit code = %d, want 1", tt.name, code)
				}
			})
			if !strings.Contains(stderr, "Error:") {
				t.Fatalf("%s stderr = %q, want Error", tt.name, stderr)
			}
		})
	}
}

func TestCmdQueryNoData(t *testing.T) {
	dataDir := t.TempDir()
	captureStdout(t, func() {
		cmdInit(dataDir)
	})

	output := captureStdout(t, func() {
		cmdQuery(dataDir, []string{"-station", "2", "-start", "2024-01-01", "-end", "2024-01-31"})
	})
	if !strings.Contains(output, "No data found for station 2") {
		t.Fatalf("cmdQuery output = %q", output)
	}
}

func TestRunQueryValidationErrors(t *testing.T) {
	initializedDir := t.TempDir()
	captureStdout(t, func() {
		cmdInit(initializedDir)
	})
	blockedParent := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blockedParent, []byte("file"), 0644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	tests := []struct {
		name    string
		dataDir string
		args    []string
	}{
		{"missing station", t.TempDir(), nil},
		{"bad flag", t.TempDir(), []string{"-unknown"}},
		{"bad start date", t.TempDir(), []string{"-station", "2", "-start", "bad", "-end", "2024-01-31"}},
		{"bad end date", t.TempDir(), []string{"-station", "2", "-start", "2024-01-01", "-end", "bad"}},
		{"metadata store open error", filepath.Join(blockedParent, "child"), []string{"-station", "2", "-start", "2024-01-01", "-end", "2024-01-31"}},
		{"bad cache size", initializedDir, []string{"-station", "2", "-start", "2024-01-01", "-end", "2024-01-31", "-cache", "bad"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := runQuery(tt.dataDir, tt.args); err == nil {
				t.Fatal("expected runQuery error")
			}
		})
	}
}

func TestRunQueryChunkLookupError(t *testing.T) {
	original := getChunksForYearRange
	t.Cleanup(func() { getChunksForYearRange = original })
	getChunksForYearRange = func(*metadata.Store, uint16, int, int, types.DataType) ([]types.ChunkInfo, error) {
		return nil, errors.New("chunk lookup failed")
	}

	dataDir := t.TempDir()
	captureStdout(t, func() {
		cmdInit(dataDir)
	})

	if err := runQuery(dataDir, []string{"-station", "2", "-start", "2024-01-01", "-end", "2024-01-31"}); err == nil {
		t.Fatal("expected chunk lookup error")
	}
}

func TestCmdQueryDailyRecordsWithPerfAndCache(t *testing.T) {
	dataDir := t.TempDir()
	captureStdout(t, func() {
		cmdInit(dataDir)
	})

	writer, err := storage.NewChunkWriter(dataDir, 1)
	if err != nil {
		t.Fatalf("NewChunkWriter() error = %v", err)
	}

	recordDate := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	records := make([]types.DailyRecord, 0, 11)
	for i := 0; i < 11; i++ {
		records = append(records, types.DailyRecord{
			Timestamp:      types.TimeToDaysSinceEpoch(recordDate.AddDate(0, 0, i)),
			StationID:      2,
			Temperature:    types.ScaleTemperature(23.4),
			ET:             types.ScaleET(4.2),
			WindSpeed:      types.ScaleWindSpeed(1.5),
			Humidity:       64,
			SolarRadiation: 22,
		})
	}
	chunkInfo, err := writer.WriteDailyChunk(2, 2024, records)
	if err != nil {
		t.Fatalf("WriteDailyChunk() error = %v", err)
	}

	store, err := metadata.NewStore(filepath.Join(dataDir, "metadata.sqlite3"))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	if err := store.SaveChunk(chunkInfo); err != nil {
		t.Fatalf("SaveChunk() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close store: %v", err)
	}

	output := captureStdout(t, func() {
		cmdQuery(dataDir, []string{
			"-station", "2",
			"-start", "2024-01-01",
			"-end", "2024-02-01",
			"-cache", "1MB",
			"-perf",
		})
	})
	for _, want := range []string{"Querying 1 chunks", "Total records: 11", "(showing first 10)", "Performance Metrics", "Cache Statistics"} {
		if !strings.Contains(output, want) {
			t.Fatalf("cmdQuery output missing %q:\n%s", want, output)
		}
	}
}

func TestCmdQueryHourlyRecords(t *testing.T) {
	dataDir := t.TempDir()
	captureStdout(t, func() {
		cmdInit(dataDir)
	})

	writer, err := storage.NewChunkWriter(dataDir, 1)
	if err != nil {
		t.Fatalf("NewChunkWriter() error = %v", err)
	}

	recordTime := time.Date(2024, 1, 15, 1, 0, 0, 0, time.UTC)
	chunkInfo, err := writer.WriteHourlyChunk(2, 2024, []types.HourlyRecord{
		{
			Timestamp:     types.TimeToHoursSinceEpoch(recordTime),
			StationID:     2,
			Temperature:   types.ScaleTemperature(23.4),
			ET:            types.ScaleHourlyET(0.1),
			WindSpeed:     types.ScaleWindSpeed(1.5),
			WindDirection: 45,
			Humidity:      64,
		},
	})
	if err != nil {
		t.Fatalf("WriteHourlyChunk() error = %v", err)
	}

	store, err := metadata.NewStore(filepath.Join(dataDir, "metadata.sqlite3"))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	if err := store.SaveChunk(chunkInfo); err != nil {
		t.Fatalf("SaveChunk() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close store: %v", err)
	}

	output := captureStdout(t, func() {
		cmdQuery(dataDir, []string{
			"-station", "2",
			"-start", "2024-01-01",
			"-end", "2024-02-01",
			"-hourly",
			"-perf",
		})
	})
	for _, want := range []string{"Querying 1 chunks", "Total records: 1", "2024-01-15 01:00", "Performance Metrics"} {
		if !strings.Contains(output, want) {
			t.Fatalf("cmdQuery hourly output missing %q:\n%s", want, output)
		}
	}
}

func TestRunQueryMissingChunkWarnings(t *testing.T) {
	for _, tt := range []struct {
		name     string
		dataType types.DataType
		args     []string
	}{
		{
			name:     "daily",
			dataType: types.DataTypeDaily,
			args:     []string{"-station", "2", "-start", "2024-01-01", "-end", "2024-02-01"},
		},
		{
			name:     "hourly",
			dataType: types.DataTypeHourly,
			args:     []string{"-station", "2", "-start", "2024-01-01", "-end", "2024-02-01", "-hourly"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dataDir := t.TempDir()
			captureStdout(t, func() {
				cmdInit(dataDir)
			})

			store, err := metadata.NewStore(filepath.Join(dataDir, "metadata.sqlite3"))
			if err != nil {
				t.Fatalf("NewStore() error = %v", err)
			}
			if err := store.SaveChunk(&types.ChunkInfo{
				StationID: 2,
				Year:      2024,
				DataType:  tt.dataType,
				FilePath:  filepath.Join(dataDir, "missing.zst"),
			}); err != nil {
				t.Fatalf("SaveChunk() error = %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatalf("Close store: %v", err)
			}

			output := captureStdout(t, func() {
				if err := runQuery(dataDir, tt.args); err != nil {
					t.Fatalf("runQuery() error = %v", err)
				}
			})
			if !strings.Contains(output, "Warning: failed to read chunk") || !strings.Contains(output, "Total records: 0") {
				t.Fatalf("runQuery output = %q", output)
			}
		})
	}
}

func TestCmdProfileLocalOutputs(t *testing.T) {
	dataDir := t.TempDir()

	statsOutput := captureStdout(t, func() {
		cmdProfile(dataDir, []string{"-stats"})
	})
	if !strings.Contains(statsOutput, "Runtime Statistics") {
		t.Fatalf("cmdProfile -stats output = %q", statsOutput)
	}

	noOptionOutput := captureStdout(t, func() {
		cmdProfile(dataDir, nil)
	})
	if !strings.Contains(noOptionOutput, "No profiling option specified") {
		t.Fatalf("cmdProfile no-option output = %q", noOptionOutput)
	}

	for _, tt := range []struct {
		name string
		flag string
		want string
	}{
		{"heap", "-heap", "Heap profile written"},
		{"allocs", "-allocs", "Allocs profile written"},
		{"goroutines", "-goroutines", "Goroutine profile written"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), tt.name+".prof")
			output := captureStdout(t, func() {
				cmdProfile(dataDir, []string{tt.flag, path})
			})
			if !strings.Contains(output, tt.want) {
				t.Fatalf("cmdProfile output = %q", output)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatalf("profile file stat error = %v", err)
			}
			if info.Size() == 0 {
				t.Fatalf("profile file %s is empty", path)
			}
		})
	}

	for _, tt := range []struct {
		name string
		flag string
		want string
	}{
		{"cpu", "-cpu", "CPU profile written"},
		{"mutex", "-mutex", "Mutex profile written"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), tt.name+".prof")
			output := captureStdout(t, func() {
				cmdProfile(dataDir, []string{tt.flag, path, "-duration", "1ms"})
			})
			if !strings.Contains(output, tt.want) {
				t.Fatalf("cmdProfile output = %q", output)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatalf("profile file stat error = %v", err)
			}
			if info.Size() == 0 {
				t.Fatalf("profile file %s is empty", path)
			}
		})
	}
}

func TestRunProfileValidationErrors(t *testing.T) {
	missingDirPath := func(name string) string {
		return filepath.Join(t.TempDir(), "missing", name+".prof")
	}

	tests := []struct {
		name string
		args []string
	}{
		{"bad flag", []string{"-unknown"}},
		{"cpu create error", []string{"-cpu", missingDirPath("cpu"), "-duration", "1ms"}},
		{"heap create error", []string{"-heap", missingDirPath("heap")}},
		{"allocs create error", []string{"-allocs", missingDirPath("allocs")}},
		{"goroutines create error", []string{"-goroutines", missingDirPath("goroutines")}},
		{"mutex create error", []string{"-mutex", missingDirPath("mutex"), "-duration", "1ms"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := runProfile(t.TempDir(), tt.args); err == nil {
				t.Fatal("expected runProfile error")
			}
		})
	}
}

func TestRunProfileStopCPUProfileError(t *testing.T) {
	original := stopCPUProfile
	t.Cleanup(func() { stopCPUProfile = original })
	stopCPUProfile = func(profiler *profilepkg.Profiler) error {
		_ = profiler.StopCPUProfile()
		return errors.New("stop failed")
	}

	if err := runProfile(t.TempDir(), []string{"-cpu", filepath.Join(t.TempDir(), "cpu.prof"), "-duration", "1ms"}); err == nil {
		t.Fatal("expected stop CPU profile error")
	}
}

func TestCmdProfileServerStopsOnSignal(t *testing.T) {
	originalNotify := notifyProfileSignal
	originalStop := stopProfileSignal
	t.Cleanup(func() {
		notifyProfileSignal = originalNotify
		stopProfileSignal = originalStop
	})

	notifyProfileSignal = func(ch chan<- os.Signal) {
		ch <- os.Interrupt
	}
	stopProfileSignal = func(ch chan<- os.Signal) {}

	output := captureStdout(t, func() {
		cmdProfile(t.TempDir(), []string{"-server", "127.0.0.1:0"})
	})
	for _, want := range []string{"pprof server started", "Press Ctrl+C to stop", "Shutting down"} {
		if !strings.Contains(output, want) {
			t.Fatalf("cmdProfile server output missing %q:\n%s", want, output)
		}
	}
}

func TestDefaultNotifyProfileSignal(t *testing.T) {
	ch := make(chan os.Signal, 1)
	notifyProfileSignal(ch)
	stopProfileSignal(ch)
}

func TestCmdVerifyValidAndInvalidChunks(t *testing.T) {
	dataDir := t.TempDir()
	stationDir := filepath.Join(dataDir, "stations", "002")
	if err := os.MkdirAll(stationDir, 0755); err != nil {
		t.Fatalf("MkdirAll station dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "stations", "README.txt"), []byte("ignore"), 0644); err != nil {
		t.Fatalf("write non-station entry: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(stationDir, "nested"), 0755); err != nil {
		t.Fatalf("MkdirAll nested chunk dir: %v", err)
	}

	compressed, err := storage.CompressLevel([]byte("valid chunk payload"), 1)
	if err != nil {
		t.Fatalf("CompressLevel() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(stationDir, "2024_daily.zst"), compressed, 0644); err != nil {
		t.Fatalf("write valid chunk: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stationDir, "ignore.txt"), []byte("not a chunk"), 0644); err != nil {
		t.Fatalf("write ignored file: %v", err)
	}

	output := captureStdout(t, func() {
		cmdVerify(dataDir)
	})
	if !strings.Contains(output, "1 OK, 0 failed") {
		t.Fatalf("cmdVerify output = %q", output)
	}
}

func TestRunVerifyInvalidChunk(t *testing.T) {
	dataDir := t.TempDir()
	stationDir := filepath.Join(dataDir, "stations", "002")
	if err := os.MkdirAll(stationDir, 0755); err != nil {
		t.Fatalf("MkdirAll station dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stationDir, "2024_daily.zst"), []byte("not compressed"), 0644); err != nil {
		t.Fatalf("write invalid chunk: %v", err)
	}

	output := captureStdout(t, func() {
		if err := runVerify(dataDir); err == nil {
			t.Fatal("expected runVerify error for invalid chunk")
		}
	})
	if !strings.Contains(output, "0 OK, 1 failed") {
		t.Fatalf("runVerify output = %q", output)
	}
}

func TestRunVerifyReadErrors(t *testing.T) {
	t.Run("station directory read error is skipped", func(t *testing.T) {
		originalReadDir := verifyReadDir
		t.Cleanup(func() { verifyReadDir = originalReadDir })

		dataDir := t.TempDir()
		stationDir := filepath.Join(dataDir, "stations", "002")
		if err := os.MkdirAll(stationDir, 0755); err != nil {
			t.Fatalf("MkdirAll station dir: %v", err)
		}

		verifyReadDir = func(name string) ([]os.DirEntry, error) {
			if filepath.Base(name) == "002" {
				return nil, errors.New("read station failed")
			}
			return os.ReadDir(name)
		}

		output := captureStdout(t, func() {
			if err := runVerify(dataDir); err != nil {
				t.Fatalf("runVerify() error = %v", err)
			}
		})
		if !strings.Contains(output, "0 OK, 0 failed") {
			t.Fatalf("runVerify output = %q", output)
		}
	})

	t.Run("chunk read error fails verification", func(t *testing.T) {
		originalReadFile := verifyReadFile
		t.Cleanup(func() { verifyReadFile = originalReadFile })

		dataDir := t.TempDir()
		stationDir := filepath.Join(dataDir, "stations", "002")
		if err := os.MkdirAll(stationDir, 0755); err != nil {
			t.Fatalf("MkdirAll station dir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(stationDir, "2024_daily.zst"), []byte("compressed"), 0644); err != nil {
			t.Fatalf("write chunk: %v", err)
		}

		verifyReadFile = func(name string) ([]byte, error) {
			return nil, errors.New("read chunk failed")
		}

		output := captureStdout(t, func() {
			if err := runVerify(dataDir); err == nil {
				t.Fatal("expected runVerify read error")
			}
		})
		if !strings.Contains(output, "read error") || !strings.Contains(output, "0 OK, 1 failed") {
			t.Fatalf("runVerify output = %q", output)
		}
	})
}

func TestFetchStationStreamingWritesAndSkipsExistingChunk(t *testing.T) {
	var requestCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if r.URL.Path != "/StationWeb/GetDataByStationNumber" {
			t.Errorf("path = %q, want /StationWeb/GetDataByStationNumber", r.URL.Path)
		}
		if r.URL.Query().Get("stationNbrs") != "2" {
			t.Errorf("stationNbrs = %q, want 2", r.URL.Query().Get("stationNbrs"))
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"Data":{"Providers":[{"Records":[{"Date":"2024-01-15","DayAirTmpAvg":{"Value":"23.4","Qc":" "},"DayAsceEto":{"Value":"4.2","Qc":" "},"DayWindSpdAvg":{"Value":"1.5","Qc":" "},"DayRelHumAvg":{"Value":"64","Qc":" "},"DaySolRadAvg":{"Value":"2.2","Qc":" "}}]}]}}`)
	}))
	defer server.Close()

	dataDir := t.TempDir()
	store, err := metadata.NewStore(filepath.Join(dataDir, "metadata.sqlite3"))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	defer store.Close()

	writer, err := storage.NewChunkWriter(dataDir, 1)
	if err != nil {
		t.Fatalf("NewChunkWriter() error = %v", err)
	}

	client := api.NewOptimizedClient("test-key")
	client.SetBaseURL(server.URL)
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)

	result := fetchStationStreaming(client, store, writer, 2, start, end, "v1", false, 0)
	if !result.success {
		t.Fatalf("fetchStationStreaming failed: %v", result.err)
	}
	if result.recordCount != 1 {
		t.Fatalf("recordCount = %d, want 1", result.recordCount)
	}
	if requestCount != 1 {
		t.Fatalf("requestCount = %d, want 1", requestCount)
	}

	result = fetchStationStreaming(client, store, writer, 2, start, end, "v1", false, 0)
	if !result.success {
		t.Fatalf("existing chunk result failed: %v", result.err)
	}
	if result.recordCount != 0 {
		t.Fatalf("existing chunk recordCount = %d, want 0", result.recordCount)
	}
	if requestCount != 1 {
		t.Fatalf("existing chunk should not refetch; requestCount = %d", requestCount)
	}
}

func TestFetchStationStreamingFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server down", http.StatusBadGateway)
	}))
	defer server.Close()

	dataDir := t.TempDir()
	store, err := metadata.NewStore(filepath.Join(dataDir, "metadata.sqlite3"))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	defer store.Close()

	writer, err := storage.NewChunkWriter(dataDir, 1)
	if err != nil {
		t.Fatalf("NewChunkWriter() error = %v", err)
	}

	client := api.NewOptimizedClient("test-key")
	client.SetBaseURL(server.URL)
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)

	result := fetchStationStreaming(client, store, writer, 2, start, end, "v1", true, 0)
	if result.success {
		t.Fatal("expected failed result")
	}
	if result.err == nil {
		t.Fatal("expected error on failed result")
	}
}

func TestFetchStationStreamingRetriesThenSucceeds(t *testing.T) {
	originalSleep := retrySleep
	originalJitter := retryJitter
	t.Cleanup(func() {
		retrySleep = originalSleep
		retryJitter = originalJitter
	})
	retrySleep = func(time.Duration) {}
	retryJitter = func(time.Duration) time.Duration { return 0 }

	var requestCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if requestCount == 1 {
			http.Error(w, "try again", http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"Data":{"Providers":[{"Records":[{"Date":"2024-01-15","DayAirTmpAvg":{"Value":"23.4","Qc":" "},"DayAsceEto":{"Value":"4.2","Qc":" "}}]}]}}`)
	}))
	defer server.Close()

	dataDir := t.TempDir()
	store, err := metadata.NewStore(filepath.Join(dataDir, "metadata.sqlite3"))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	defer store.Close()

	writer, err := storage.NewChunkWriter(dataDir, 1)
	if err != nil {
		t.Fatalf("NewChunkWriter() error = %v", err)
	}

	client := api.NewOptimizedClient("test-key")
	client.SetBaseURL(server.URL)
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)

	result := fetchStationStreaming(client, store, writer, 2, start, end, "v1", false, 1)
	if !result.success {
		t.Fatalf("fetchStationStreaming retry result failed: %v", result.err)
	}
	if requestCount != 2 {
		t.Fatalf("requestCount = %d, want 2", requestCount)
	}
}

func TestFetchStationStreamingWriteAndSaveErrors(t *testing.T) {
	server := newMockCIMISServer(t)
	defer server.Close()

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)

	t.Run("write error", func(t *testing.T) {
		dataDir := t.TempDir()
		store, err := metadata.NewStore(filepath.Join(dataDir, "metadata.sqlite3"))
		if err != nil {
			t.Fatalf("NewStore() error = %v", err)
		}
		defer store.Close()

		writer, err := storage.NewChunkWriter(dataDir, 1)
		if err != nil {
			t.Fatalf("NewChunkWriter() error = %v", err)
		}
		if err := os.WriteFile(filepath.Join(dataDir, "stations"), []byte("file"), 0644); err != nil {
			t.Fatalf("write stations file: %v", err)
		}

		client := api.NewOptimizedClient("test-key")
		client.SetBaseURL(server.URL)

		result := fetchStationStreaming(client, store, writer, 2, start, end, "v1", false, 0)
		if result.success || result.err == nil {
			t.Fatalf("expected write error result, got %+v", result)
		}
	})

	t.Run("save metadata error", func(t *testing.T) {
		dataDir := t.TempDir()
		store, err := metadata.NewStore(filepath.Join(dataDir, "metadata.sqlite3"))
		if err != nil {
			t.Fatalf("NewStore() error = %v", err)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("Close store: %v", err)
		}

		writer, err := storage.NewChunkWriter(dataDir, 1)
		if err != nil {
			t.Fatalf("NewChunkWriter() error = %v", err)
		}

		client := api.NewOptimizedClient("test-key")
		client.SetBaseURL(server.URL)

		result := fetchStationStreaming(client, store, writer, 2, start, end, "v1", false, 0)
		if result.success || result.err == nil {
			t.Fatalf("expected save metadata error result, got %+v", result)
		}
	})
}

func installMockCIMISClients(t *testing.T, serverURL string) {
	t.Helper()

	originalAPI := newAPIClient
	originalOptimized := newOptimizedAPIClient
	t.Cleanup(func() {
		newAPIClient = originalAPI
		newOptimizedAPIClient = originalOptimized
	})

	newAPIClient = func(appKey string) *api.Client {
		client := api.NewClient(appKey)
		client.SetBaseURL(serverURL)
		return client
	}
	newOptimizedAPIClient = func(appKey string) *api.OptimizedClient {
		client := api.NewOptimizedClient(appKey)
		client.SetBaseURL(serverURL)
		return client
	}
}

func newMockCIMISServer(t *testing.T) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/StationWeb/GetDataByStationNumber" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}

		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("isHourly") == "true" {
			fmt.Fprint(w, `{"Data":{"Providers":[{"Records":[{"Date":"2024-01-15","Hour":"0100","Station":"2","HlyAirTmp":{"Value":"23.4","Qc":" "},"HlyAsceEto":{"Value":"0.1","Qc":" "},"HlyWindSpd":{"Value":"1.5","Qc":" "},"HlyWindDir":{"Value":"90","Qc":" "},"HlyRelHum":{"Value":"64","Qc":" "},"HlySolRad":{"Value":"12","Qc":" "},"HlyPrecip":{"Value":"0","Qc":" "},"HlyVapPres":{"Value":"8","Qc":" "}}]}]}}`)
			return
		}

		fmt.Fprint(w, `{"Data":{"Providers":[{"Records":[{"Date":"2024-01-15","Station":"2","DayAirTmpAvg":{"Value":"23.4","Qc":" "},"DayAsceEto":{"Value":"4.2","Qc":" "},"DayWindSpdAvg":{"Value":"1.5","Qc":" "},"DayRelHumAvg":{"Value":"64","Qc":" "},"DaySolRadAvg":{"Value":"2.2","Qc":" "},"DayPrecip":{"Value":"0","Qc":" "}}]}]}}`)
	}))
}

func newEmptyCIMISServer(t *testing.T) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"Data":{"Providers":[{"Records":[]}]}}`)
	}))
}

func TestCmdFetchDailyAndHourly(t *testing.T) {
	server := newMockCIMISServer(t)
	defer server.Close()
	installMockCIMISClients(t, server.URL)

	dailyOutput := captureStdout(t, func() {
		cmdFetch(t.TempDir(), "test-key", []string{"-station", "2", "-days", "1"})
	})
	if !strings.Contains(dailyOutput, "Fetched 1 daily records for station 2") {
		t.Fatalf("cmdFetch daily output = %q", dailyOutput)
	}

	hourlyOutput := captureStdout(t, func() {
		cmdFetch(t.TempDir(), "test-key", []string{"-station", "2", "-days", "1", "-hourly"})
	})
	if !strings.Contains(hourlyOutput, "Fetched 1 hourly records for station 2") {
		t.Fatalf("cmdFetch hourly output = %q", hourlyOutput)
	}
}

func TestRunFetchValidationErrors(t *testing.T) {
	tests := []struct {
		name   string
		appKey string
		args   []string
	}{
		{"missing app key", "", []string{"-station", "2"}},
		{"missing station", "test-key", nil},
		{"bad flag", "test-key", []string{"-unknown"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := runFetch(t.TempDir(), tt.appKey, tt.args); err == nil {
				t.Fatal("expected runFetch error")
			}
		})
	}
}

func TestRunFetchAPIErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server down", http.StatusBadGateway)
	}))
	defer server.Close()
	installMockCIMISClients(t, server.URL)

	tests := []struct {
		name string
		args []string
	}{
		{"daily", []string{"-station", "2", "-days", "1"}},
		{"hourly", []string{"-station", "2", "-days", "1", "-hourly"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := runFetch(t.TempDir(), "test-key", tt.args); err == nil {
				t.Fatal("expected runFetch API error")
			}
		})
	}
}

func TestDefaultRetryJitter(t *testing.T) {
	got := retryJitter(2 * time.Second)
	if got < 0 || got >= time.Second {
		t.Fatalf("retryJitter = %v, want [0, 1s)", got)
	}
}

func TestCmdFetchStreamingWithMockAPI(t *testing.T) {
	server := newMockCIMISServer(t)
	defer server.Close()
	installMockCIMISClients(t, server.URL)

	output := captureStdout(t, func() {
		cmdFetchStreaming(t.TempDir(), "test-key", []string{
			"-stations", "2",
			"-start", "2024-01-01",
			"-end", "2024-01-31",
			"-concurrency", "1",
			"-retries", "0",
			"-perf",
			"-allocs",
		})
	})
	for _, want := range []string{"Fetch Streaming Summary", "Successful: 1", "Total records: 1", "Performance Metrics"} {
		if !strings.Contains(output, want) {
			t.Fatalf("cmdFetchStreaming output missing %q:\n%s", want, output)
		}
	}
}

func TestRunFetchStreamingValidationErrors(t *testing.T) {
	blockedParent := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blockedParent, []byte("file"), 0644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	metadataDirOut := t.TempDir()
	if err := os.MkdirAll(filepath.Join(metadataDirOut, "metadata.sqlite3"), 0755); err != nil {
		t.Fatalf("MkdirAll metadata dir: %v", err)
	}

	tests := []struct {
		name   string
		appKey string
		args   []string
	}{
		{"missing app key", "", []string{"-stations", "2"}},
		{"missing stations", "test-key", nil},
		{"bad station list", "test-key", []string{"-stations", "abc"}},
		{"reversed station range", "test-key", []string{"-stations", "5-3"}},
		{"bad start date", "test-key", []string{"-stations", "2", "-start", "bad", "-end", "2024-01-31"}},
		{"bad end date", "test-key", []string{"-stations", "2", "-start", "2024-01-01", "-end", "bad"}},
		{"bad format", "test-key", []string{"-stations", "2", "-format", "v3"}},
		{"output directory create error", "test-key", []string{"-stations", "2", "-out", filepath.Join(blockedParent, "child")}},
		{"metadata store open error", "test-key", []string{"-stations", "2", "-out", metadataDirOut}},
		{"bad flag", "test-key", []string{"-unknown"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("CIMIS_APP_KEY", "")
			if err := runFetchStreaming(t.TempDir(), tt.appKey, tt.args); err == nil {
				t.Fatal("expected runFetchStreaming error")
			}
		})
	}
}

func TestRunFetchStreamingChunkWriterError(t *testing.T) {
	original := newChunkWriter
	t.Cleanup(func() { newChunkWriter = original })

	newChunkWriter = func(string, int) (*storage.ChunkWriter, error) {
		return nil, errors.New("writer failed")
	}

	if err := runFetchStreaming(t.TempDir(), "test-key", []string{"-stations", "2"}); err == nil {
		t.Fatal("expected chunk writer error")
	}
}

func TestCmdFetchStreamingFailureSummary(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server down", http.StatusBadGateway)
	}))
	defer server.Close()
	installMockCIMISClients(t, server.URL)

	output := captureStdout(t, func() {
		cmdFetchStreaming(t.TempDir(), "test-key", []string{
			"-stations", "2",
			"-start", "2024-01-01",
			"-end", "2024-01-31",
			"-concurrency", "1",
			"-retries", "0",
			"-perf",
		})
	})
	for _, want := range []string{"Successful: 0", "Failed: 1", "Warning: 1 station"} {
		if !strings.Contains(output, want) {
			t.Fatalf("cmdFetchStreaming failure output missing %q:\n%s", want, output)
		}
	}
}

func TestCmdIngestWithMockAPI(t *testing.T) {
	server := newMockCIMISServer(t)
	defer server.Close()
	installMockCIMISClients(t, server.URL)

	output := captureStdout(t, func() {
		if err := runIngest(t.TempDir(), "test-key", []string{"-station", "2", "-year", "2024"}); err != nil {
			t.Fatalf("runIngest() error = %v", err)
		}
	})
	if !strings.Contains(output, "Ingested 1 daily records") {
		t.Fatalf("cmdIngest output = %q", output)
	}
}

func TestRunIngestValidationAndFetchErrors(t *testing.T) {
	blockedParent := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blockedParent, []byte("file"), 0644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	metadataDirData := t.TempDir()
	if err := os.MkdirAll(filepath.Join(metadataDirData, "metadata.sqlite3"), 0755); err != nil {
		t.Fatalf("MkdirAll metadata dir: %v", err)
	}

	tests := []struct {
		name   string
		appKey string
		args   []string
	}{
		{"missing app key", "", []string{"-station", "2"}},
		{"missing station", "test-key", nil},
		{"bad flag", "test-key", []string{"-unknown"}},
		{"data directory create error", "test-key", []string{"-station", "2"}},
		{"metadata store open error", "test-key", []string{"-station", "2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataDir := t.TempDir()
			if tt.name == "data directory create error" {
				dataDir = filepath.Join(blockedParent, "child")
			} else if tt.name == "metadata store open error" {
				dataDir = metadataDirData
			}
			if err := runIngest(dataDir, tt.appKey, tt.args); err == nil {
				t.Fatal("expected runIngest error")
			}
		})
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server down", http.StatusBadGateway)
	}))
	defer server.Close()
	installMockCIMISClients(t, server.URL)
	if err := runIngest(t.TempDir(), "test-key", []string{"-station", "2", "-year", "2024"}); err == nil {
		t.Fatal("expected runIngest API error")
	}
}

func TestRunIngestWriteError(t *testing.T) {
	server := newMockCIMISServer(t)
	defer server.Close()
	installMockCIMISClients(t, server.URL)

	dataDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dataDir, "stations"), []byte("file"), 0644); err != nil {
		t.Fatalf("write stations file: %v", err)
	}

	if err := runIngest(dataDir, "test-key", []string{"-station", "2", "-year", "2024"}); err == nil {
		t.Fatal("expected runIngest write error")
	}
}

func TestRunIngestChunkWriterAndSaveErrors(t *testing.T) {
	server := newMockCIMISServer(t)
	defer server.Close()
	installMockCIMISClients(t, server.URL)

	originalWriter := newChunkWriter
	originalSave := saveChunkMetadata
	t.Cleanup(func() {
		newChunkWriter = originalWriter
		saveChunkMetadata = originalSave
	})

	t.Run("chunk writer error", func(t *testing.T) {
		newChunkWriter = func(string, int) (*storage.ChunkWriter, error) {
			return nil, errors.New("writer failed")
		}
		saveChunkMetadata = originalSave

		if err := runIngest(t.TempDir(), "test-key", []string{"-station", "2", "-year", "2024"}); err == nil {
			t.Fatal("expected chunk writer error")
		}
	})

	t.Run("metadata save error", func(t *testing.T) {
		newChunkWriter = originalWriter
		saveChunkMetadata = func(*metadata.Store, *types.ChunkInfo) error {
			return errors.New("save failed")
		}

		if err := runIngest(t.TempDir(), "test-key", []string{"-station", "2", "-year", "2024"}); err == nil {
			t.Fatal("expected metadata save error")
		}
	})
}

func TestCmdIngestSkipExistingAndNoRecords(t *testing.T) {
	dataDir := t.TempDir()
	store, err := metadata.NewStore(filepath.Join(dataDir, "metadata.sqlite3"))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	if err := store.SaveChunk(&types.ChunkInfo{StationID: 2, Year: 2024, DataType: types.DataTypeDaily}); err != nil {
		t.Fatalf("SaveChunk() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close store: %v", err)
	}

	server := newMockCIMISServer(t)
	defer server.Close()
	installMockCIMISClients(t, server.URL)

	output := captureStdout(t, func() {
		if err := runIngest(dataDir, "test-key", []string{"-station", "2", "-year", "2024"}); err != nil {
			t.Fatalf("runIngest() error = %v", err)
		}
	})
	if !strings.Contains(output, "already exists") {
		t.Fatalf("cmdIngest skip output = %q", output)
	}

	emptyServer := newEmptyCIMISServer(t)
	defer emptyServer.Close()
	installMockCIMISClients(t, emptyServer.URL)

	output = captureStdout(t, func() {
		if err := runIngest(t.TempDir(), "test-key", []string{"-station", "2", "-year", "2024"}); err != nil {
			t.Fatalf("runIngest() error = %v", err)
		}
	})
	if !strings.Contains(output, "No records found") {
		t.Fatalf("cmdIngest no-record output = %q", output)
	}

	output = captureStdout(t, func() {
		if err := runIngest(t.TempDir(), "test-key", []string{"-station", "2"}); err != nil {
			t.Fatalf("runIngest() error = %v", err)
		}
	})
	if !strings.Contains(output, "No records found") {
		t.Fatalf("cmdIngest default-year no-record output = %q", output)
	}
}

func TestCmdIngestOptimizedWithMockAPI(t *testing.T) {
	server := newMockCIMISServer(t)
	defer server.Close()
	installMockCIMISClients(t, server.URL)

	output := captureStdout(t, func() {
		cmdIngestOptimized(t.TempDir(), "test-key", []string{"-station", "2", "-year", "2024"})
	})
	if !strings.Contains(output, "Ingested 1 daily records") {
		t.Fatalf("cmdIngestOptimized output = %q", output)
	}
	if !strings.Contains(output, "Compression verification passed") {
		t.Fatalf("cmdIngestOptimized output = %q", output)
	}
}

func TestRunIngestOptimizedValidationAndFetchErrors(t *testing.T) {
	blockedParent := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blockedParent, []byte("file"), 0644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	metadataDirData := t.TempDir()
	if err := os.MkdirAll(filepath.Join(metadataDirData, "metadata.sqlite3"), 0755); err != nil {
		t.Fatalf("MkdirAll metadata dir: %v", err)
	}

	tests := []struct {
		name   string
		appKey string
		args   []string
	}{
		{"missing app key", "", []string{"-station", "2"}},
		{"missing station", "test-key", nil},
		{"bad flag", "test-key", []string{"-unknown"}},
		{"data directory create error", "test-key", []string{"-station", "2"}},
		{"metadata store open error", "test-key", []string{"-station", "2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataDir := t.TempDir()
			if tt.name == "data directory create error" {
				dataDir = filepath.Join(blockedParent, "child")
			} else if tt.name == "metadata store open error" {
				dataDir = metadataDirData
			}
			if err := runIngestOptimized(dataDir, tt.appKey, tt.args); err == nil {
				t.Fatal("expected runIngestOptimized error")
			}
		})
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server down", http.StatusBadGateway)
	}))
	defer server.Close()
	installMockCIMISClients(t, server.URL)
	if err := runIngestOptimized(t.TempDir(), "test-key", []string{"-station", "2", "-year", "2024"}); err == nil {
		t.Fatal("expected runIngestOptimized API error")
	}
}

func TestCmdIngestOptimizedNoRecords(t *testing.T) {
	server := newEmptyCIMISServer(t)
	defer server.Close()
	installMockCIMISClients(t, server.URL)

	output := captureStdout(t, func() {
		cmdIngestOptimized(t.TempDir(), "test-key", []string{"-station", "2", "-year", "2024"})
	})
	if !strings.Contains(output, "No records to ingest") {
		t.Fatalf("cmdIngestOptimized output = %q", output)
	}

	output = captureStdout(t, func() {
		cmdIngestOptimized(t.TempDir(), "test-key", []string{"-station", "2"})
	})
	if !strings.Contains(output, "No records to ingest") {
		t.Fatalf("cmdIngestOptimized default-year output = %q", output)
	}
}

func TestRunIngestOptimizedFilesystemErrors(t *testing.T) {
	server := newMockCIMISServer(t)
	defer server.Close()
	installMockCIMISClients(t, server.URL)

	t.Run("station directory create error", func(t *testing.T) {
		dataDir := t.TempDir()
		stationsDir := filepath.Join(dataDir, "stations")
		if err := os.MkdirAll(stationsDir, 0755); err != nil {
			t.Fatalf("MkdirAll stations dir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(stationsDir, "002"), []byte("file"), 0644); err != nil {
			t.Fatalf("write station path file: %v", err)
		}

		if err := runIngestOptimized(dataDir, "test-key", []string{"-station", "2", "-year", "2024"}); err == nil {
			t.Fatal("expected station directory create error")
		}
	})

	t.Run("chunk write error", func(t *testing.T) {
		dataDir := t.TempDir()
		chunkPath := filepath.Join(dataDir, "stations", "002", "2024_optimized.zst")
		if err := os.MkdirAll(chunkPath, 0755); err != nil {
			t.Fatalf("MkdirAll chunk path as dir: %v", err)
		}

		if err := runIngestOptimized(dataDir, "test-key", []string{"-station", "2", "-year", "2024"}); err == nil {
			t.Fatal("expected chunk write error")
		}
	})
}

func TestRunIngestOptimizedProcessingErrors(t *testing.T) {
	server := newMockCIMISServer(t)
	defer server.Close()
	installMockCIMISClients(t, server.URL)

	originalOptimize := optimizeColumns
	originalCompress := compressLevel
	originalDecompress := decompressData
	t.Cleanup(func() {
		optimizeColumns = originalOptimize
		compressLevel = originalCompress
		decompressData = originalDecompress
	})

	tests := []struct {
		name  string
		setup func()
	}{
		{"optimize error", func() {
			optimizeColumns = func(*storage.ColumnarDailyData, uint16) ([]byte, *storage.ColumnarMeta, error) {
				return nil, nil, errors.New("optimize failed")
			}
		}},
		{"compress error", func() {
			compressLevel = func([]byte, int) ([]byte, error) {
				return nil, errors.New("compress failed")
			}
		}},
		{"decompress error", func() {
			decompressData = func([]byte, []byte) ([]byte, error) {
				return nil, errors.New("decompress failed")
			}
		}},
		{"decompression mismatch", func() {
			decompressData = func([]byte, []byte) ([]byte, error) {
				return []byte("mismatch"), nil
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			optimizeColumns = originalOptimize
			compressLevel = originalCompress
			decompressData = originalDecompress
			tt.setup()

			if err := runIngestOptimized(t.TempDir(), "test-key", []string{"-station", "2", "-year", "2024"}); err == nil {
				t.Fatal("expected runIngestOptimized processing error")
			}
		})
	}
}

func TestProfileMemoryDuringIngestWithMockAPI(t *testing.T) {
	server := newMockCIMISServer(t)
	defer server.Close()
	installMockCIMISClients(t, server.URL)
	t.Setenv("CIMIS_APP_KEY", "test-key")

	output := captureStdout(t, func() {
		profileMemoryDuringIngest(2, 2024, t.TempDir())
	})
	for _, want := range []string{"Memory Before Ingestion", "Memory After Ingestion", "Memory Profiling Summary", "Records ingested: 1"} {
		if !strings.Contains(output, want) {
			t.Fatalf("profileMemoryDuringIngest output missing %q:\n%s", want, output)
		}
	}
}

func TestProfileMemoryDuringIngestMissingKeyFatal(t *testing.T) {
	withFatalPanic(t)
	t.Setenv("CIMIS_APP_KEY", "")

	expectPanic(t, func() {
		captureStdout(t, func() {
			profileMemoryDuringIngest(2, 2024, t.TempDir())
		})
	})
}

func TestProfileMemoryDuringIngestFatalErrors(t *testing.T) {
	withFatalPanic(t)
	t.Setenv("CIMIS_APP_KEY", "test-key")

	t.Run("fetch error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "server down", http.StatusBadGateway)
		}))
		defer server.Close()
		installMockCIMISClients(t, server.URL)

		expectPanic(t, func() {
			captureStdout(t, func() {
				profileMemoryDuringIngest(2, 2024, t.TempDir())
			})
		})
	})

	server := newMockCIMISServer(t)
	defer server.Close()
	installMockCIMISClients(t, server.URL)

	originalOptimize := optimizeColumns
	originalCompress := compressLevel
	t.Cleanup(func() {
		optimizeColumns = originalOptimize
		compressLevel = originalCompress
	})

	t.Run("optimize error", func(t *testing.T) {
		optimizeColumns = func(*storage.ColumnarDailyData, uint16) ([]byte, *storage.ColumnarMeta, error) {
			return nil, nil, errors.New("optimize failed")
		}
		compressLevel = originalCompress
		expectPanic(t, func() {
			captureStdout(t, func() {
				profileMemoryDuringIngest(2, 2024, t.TempDir())
			})
		})
	})

	t.Run("compress error", func(t *testing.T) {
		optimizeColumns = originalOptimize
		compressLevel = func([]byte, int) ([]byte, error) {
			return nil, errors.New("compress failed")
		}
		expectPanic(t, func() {
			captureStdout(t, func() {
				profileMemoryDuringIngest(2, 2024, t.TempDir())
			})
		})
	})
}

func TestProfileMemoryDuringIngestNegativeMemoryDelta(t *testing.T) {
	server := newMockCIMISServer(t)
	defer server.Close()
	installMockCIMISClients(t, server.URL)
	t.Setenv("CIMIS_APP_KEY", "test-key")

	originalStats := getProfileMemoryStats
	t.Cleanup(func() { getProfileMemoryStats = originalStats })

	stats := []profilepkg.MemoryStats{
		{Alloc: 1024, NumGC: 2},
		{Alloc: 512, NumGC: 3, PauseNs: 1000},
	}
	getProfileMemoryStats = func() profilepkg.MemoryStats {
		next := stats[0]
		if len(stats) > 1 {
			stats = stats[1:]
		}
		return next
	}

	output := captureStdout(t, func() {
		profileMemoryDuringIngest(2, 2024, t.TempDir())
	})
	if !strings.Contains(output, "Memory allocated: 0.00 MB") {
		t.Fatalf("profileMemoryDuringIngest output = %q", output)
	}
}

func TestProfileMemoryDuringIngestPositiveMemoryDelta(t *testing.T) {
	server := newMockCIMISServer(t)
	defer server.Close()
	installMockCIMISClients(t, server.URL)
	t.Setenv("CIMIS_APP_KEY", "test-key")

	originalStats := getProfileMemoryStats
	t.Cleanup(func() { getProfileMemoryStats = originalStats })

	stats := []profilepkg.MemoryStats{
		{Alloc: 512, NumGC: 2},
		{Alloc: 2048, NumGC: 3, PauseNs: 1000},
	}
	getProfileMemoryStats = func() profilepkg.MemoryStats {
		next := stats[0]
		if len(stats) > 1 {
			stats = stats[1:]
		}
		return next
	}

	output := captureStdout(t, func() {
		profileMemoryDuringIngest(2, 2024, t.TempDir())
	})
	if !strings.Contains(output, "Records ingested: 1") {
		t.Fatalf("profileMemoryDuringIngest output = %q", output)
	}
}

func TestRunProfileMemoryIngestNoRecords(t *testing.T) {
	server := newEmptyCIMISServer(t)
	defer server.Close()
	installMockCIMISClients(t, server.URL)
	t.Setenv("CIMIS_APP_KEY", "test-key")

	output := captureStdout(t, func() {
		if err := runProfile(t.TempDir(), []string{"-station", "2", "-year", "2024"}); err != nil {
			t.Fatalf("runProfile() error = %v", err)
		}
	})
	if !strings.Contains(output, "No records to ingest") {
		t.Fatalf("runProfile ingest output = %q", output)
	}
}
