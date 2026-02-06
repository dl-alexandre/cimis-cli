package main

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"
)

type AllocMetrics struct {
	BeforeAlloc    uint64
	BeforeHeap     uint64
	AfterAlloc     uint64
	AfterHeap      uint64
	DeltaAlloc     uint64
	DeltaHeap      uint64
	Objects        uint64
	BytesPerRecord float64
}

func (a *AllocMetrics) String() string {
	return fmt.Sprintf("Alloc: %d bytes, Heap: %d bytes, Objects: %d, PerRecord: %.2f bytes",
		a.DeltaAlloc, a.DeltaHeap, a.Objects, a.BytesPerRecord)
}

func CaptureAllocMetrics() func() AllocMetrics {
	runtime.GC()
	time.Sleep(10 * time.Millisecond)

	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)

	return func() AllocMetrics {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
		runtime.ReadMemStats(&m2)

		return AllocMetrics{
			BeforeAlloc: m1.Alloc,
			BeforeHeap:  m1.HeapInuse,
			AfterAlloc:  m2.Alloc,
			AfterHeap:   m2.HeapInuse,
			DeltaAlloc:  m2.Alloc - m1.Alloc,
			DeltaHeap:   m2.HeapInuse - m1.HeapInuse,
			Objects:     m2.HeapObjects - m1.HeapObjects,
		}
	}
}

type RetryableError struct {
	Err         error
	StatusCode  int
	ShouldRetry bool
}

func (e *RetryableError) Error() string {
	if e.ShouldRetry {
		return fmt.Sprintf("retryable: %v (status: %d)", e.Err, e.StatusCode)
	}
	return fmt.Sprintf("non-retryable: %v (status: %d)", e.Err, e.StatusCode)
}

func ClassifyRetryableError(err error, statusCode int) *RetryableError {
	if err == nil {
		return nil
	}

	if statusCode >= 400 && statusCode < 500 && statusCode != 429 {
		return &RetryableError{Err: err, StatusCode: statusCode, ShouldRetry: false}
	}

	if statusCode == 429 || statusCode >= 500 {
		return &RetryableError{Err: err, StatusCode: statusCode, ShouldRetry: true}
	}

	errStr := err.Error()
	if containsAny(errStr, []string{"timeout", "connection refused", "connection reset", "EOF", "broken pipe", "no such host"}) {
		return &RetryableError{Err: err, StatusCode: statusCode, ShouldRetry: true}
	}

	return &RetryableError{Err: err, StatusCode: statusCode, ShouldRetry: false}
}

func containsAny(s string, substrs []string) bool {
	for _, substr := range substrs {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}

type JSONStationResult struct {
	StationID        uint16        `json:"station_id"`
	Year             int           `json:"year"`
	Success          bool          `json:"success"`
	Records          int           `json:"records"`
	Attempts         int           `json:"attempts"`
	Error            string        `json:"error,omitempty"`
	Timings          Timings       `json:"timings"`
	BytesTransferred int64         `json:"bytes_transferred"`
	BytesCompressed  int64         `json:"bytes_compressed,omitempty"`
	CompressionRatio float64       `json:"compression_ratio,omitempty"`
	AllocMetrics     *AllocMetrics `json:"alloc_metrics,omitempty"`
}

type Timings struct {
	DNS    time.Duration `json:"dns_ms"`
	TCP    time.Duration `json:"tcp_ms"`
	TLS    time.Duration `json:"tls_ms"`
	TTFB   time.Duration `json:"ttfb_ms"`
	Read   time.Duration `json:"read_ms"`
	Decode time.Duration `json:"decode_ms"`
	Write  time.Duration `json:"write_ms"`
	Total  time.Duration `json:"total_ms"`
}

type JSONOutputSummary struct {
	Timestamp     time.Time           `json:"timestamp"`
	TotalStations int                 `json:"total_stations"`
	Successful    int                 `json:"successful"`
	Failed        int                 `json:"failed"`
	TotalRecords  int                 `json:"total_records"`
	Results       []JSONStationResult `json:"results"`
}

func PrintJSONResults(results []JSONStationResult, totalTime time.Duration) {
	summary := JSONOutputSummary{
		Timestamp:     time.Now(),
		TotalStations: len(results),
		Results:       results,
	}

	for _, r := range results {
		if r.Success {
			summary.Successful++
			summary.TotalRecords += r.Records
		} else {
			summary.Failed++
		}
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	encoder.Encode(summary)
}

func VerifyAtomicWrite(filepath string) bool {
	info, err := os.Stat(filepath)
	if err != nil {
		return false
	}
	if info.Size() == 0 {
		return false
	}
	return true
}
