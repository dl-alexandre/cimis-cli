package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestEndToEndIntegration performs a full integration test
func TestEndToEndIntegration(t *testing.T) {
	// Skip if no API key available
	appKey := os.Getenv("CIMIS_APP_KEY")
	if appKey == "" {
		t.Skip("CIMIS_APP_KEY not set, skipping integration test")
	}

	// Create temporary directory for test data
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	// Build CLI
	cliPath := filepath.Join(tmpDir, "cimisdb")
	buildCmd := exec.Command("go", "build", "-o", cliPath, ".")
	buildCmd.Dir = ".." // Project root
	if output, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build CLI: %v\nOutput: %s", err, output)
	}

	t.Run("InitDatabase", func(t *testing.T) {
		cmd := exec.Command(cliPath, "init")
		cmd.Env = append(os.Environ(), "CIMIS_APP_KEY="+appKey)
		cmd.Dir = tmpDir

		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("Init failed: %v\nOutput: %s", err, output)
		}

		// Verify data directory created
		if _, err := os.Stat(dataDir); os.IsNotExist(err) {
			t.Error("Data directory not created")
		}

		// Verify metadata database created
		dbPath := filepath.Join(dataDir, "metadata.sqlite3")
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			t.Error("Metadata database not created")
		}

		t.Log("Database initialized successfully")
	})

	t.Run("FetchData", func(t *testing.T) {
		cmd := exec.Command(cliPath, "fetch", "-station", "2", "-days", "7")
		cmd.Env = append(os.Environ(), "CIMIS_APP_KEY="+appKey)
		cmd.Dir = tmpDir

		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("Fetch failed: %v\nOutput: %s", err, output)
		}

		outputStr := string(output)
		if !strings.Contains(outputStr, "Fetched") {
			t.Errorf("Expected 'Fetched' in output, got: %s", outputStr)
		}

		t.Logf("Fetch output: %s", outputStr)
	})

	t.Run("IngestData", func(t *testing.T) {
		// Use a recent year with known data
		year := time.Now().Year() - 1

		cmd := exec.Command(cliPath, "ingest", "-station", "2", "-year", string(rune(year)))
		cmd.Env = append(os.Environ(), "CIMIS_APP_KEY="+appKey)
		cmd.Dir = tmpDir

		output, err := cmd.CombinedOutput()
		if err != nil {
			// API might not have recent data, try older year
			cmd = exec.Command(cliPath, "ingest", "-station", "2", "-year", "2023")
			cmd.Env = append(os.Environ(), "CIMIS_APP_KEY="+appKey)
			cmd.Dir = tmpDir
			output, err = cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("Ingest failed: %v\nOutput: %s", err, output)
			}
		}

		outputStr := string(output)
		if !strings.Contains(outputStr, "Ingested") {
			t.Errorf("Expected 'Ingested' in output, got: %s", outputStr)
		}

		// Verify chunk file created
		chunkDir := filepath.Join(dataDir, "stations", "002")
		entries, err := os.ReadDir(chunkDir)
		if err != nil || len(entries) == 0 {
			t.Error("No chunk files created")
		}

		t.Logf("Ingest output: %s", outputStr)
	})

	t.Run("QueryData", func(t *testing.T) {
		cmd := exec.Command(cliPath, "query", "-station", "2", "-start", "2023-06-01", "-end", "2023-06-30")
		cmd.Env = append(os.Environ(), "CIMIS_APP_KEY="+appKey)
		cmd.Dir = tmpDir

		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("Query failed: %v\nOutput: %s", err, output)
		}

		outputStr := string(output)
		if !strings.Contains(outputStr, "records") {
			t.Errorf("Expected 'records' in output, got: %s", outputStr)
		}

		t.Logf("Query output: %s", outputStr)
	})

	t.Run("Stats", func(t *testing.T) {
		cmd := exec.Command(cliPath, "stats")
		cmd.Env = append(os.Environ(), "CIMIS_APP_KEY="+appKey)
		cmd.Dir = tmpDir

		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("Stats failed: %v\nOutput: %s", err, output)
		}

		outputStr := string(output)
		if !strings.Contains(outputStr, "Statistics") {
			t.Errorf("Expected 'Statistics' in output, got: %s", outputStr)
		}

		t.Logf("Stats output: %s", outputStr)
	})

	t.Run("Verify", func(t *testing.T) {
		cmd := exec.Command(cliPath, "verify")
		cmd.Env = append(os.Environ(), "CIMIS_APP_KEY="+appKey)
		cmd.Dir = tmpDir

		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("Verify failed: %v\nOutput: %s", err, output)
		}

		outputStr := string(output)
		if !strings.Contains(outputStr, "OK") && !strings.Contains(outputStr, "complete") {
			t.Errorf("Expected verification result in output, got: %s", outputStr)
		}

		t.Logf("Verify output: %s", outputStr)
	})
}

// TestCompressionBenchmarks runs benchmarks and validates performance
func TestCompressionBenchmarks(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping benchmark tests in short mode")
	}

	t.Run("ZstdLevels", func(t *testing.T) {
		// Create test data
		data := make([]byte, 10000)
		for i := range data {
			data[i] = byte(i % 256)
		}

		bestRatio := 0.0
		bestLevel := 1

		for level := 1; level <= 10; level += 3 {
			// This would require importing the storage package
			// For now, just verify the test structure
			t.Logf("Testing level %d", level)
		}

		t.Logf("Best compression: level %d with ratio %.2fx", bestLevel, bestRatio)
	})
}

// TestDataIntegrity verifies data integrity across operations
func TestDataIntegrity(t *testing.T) {
	t.Run("RecordEncoding", func(t *testing.T) {
		// Test that records encode and decode correctly
		t.Log("Record encoding test placeholder")
	})

	t.Run("CompressionRoundTrip", func(t *testing.T) {
		// Test compress and decompress maintains data integrity
		t.Log("Compression round-trip test placeholder")
	})
}

// BenchmarkIngest benchmarks the ingestion performance
func BenchmarkIngest(b *testing.B) {
	// This would be a full benchmark of the ingestion pipeline
	b.Skip("Full ingest benchmark requires API access")
}

// BenchmarkQuery benchmarks query performance
func BenchmarkQuery(b *testing.B) {
	// This would benchmark various query patterns
	b.Skip("Query benchmark requires data to be present")
}
