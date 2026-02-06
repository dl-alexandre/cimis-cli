// Package api provides optimized CIMIS API client with streaming JSON decode.
// This version minimizes allocations and reduces fetch latency.
package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/dl-alexandre/cimis-tsdb/types"
)

// ReadBufferSize is the buffer size for JSON streaming (8KB).
const (
	streamingTimeout = 30 * time.Second
	readBufferSize   = 8192
)

// OptimizedHTTPTransport returns a tuned http.Transport for CIMIS API.
func OptimizedHTTPTransport() *http.Transport {
	return &http.Transport{
		// Connection pooling
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 20,
		IdleConnTimeout:     90 * time.Second,

		// Timeouts
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,

		// Enable HTTP/2
		ForceAttemptHTTP2: true,

		// Connection settings
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
}

// OptimizedClient is a high-performance CIMIS API client.
type OptimizedClient struct {
	appKey     string
	httpClient *http.Client
	baseURL    string

	// Buffer pool for JSON decode
	bufferPool sync.Pool
}

// NewOptimizedClient creates a high-performance API client.
func NewOptimizedClient(appKey string) *OptimizedClient {
	return &OptimizedClient{
		appKey: appKey,
		httpClient: &http.Client{
			Transport: OptimizedHTTPTransport(),
			Timeout:   streamingTimeout,
		},
		baseURL: "http://et.water.ca.gov/api/data",
		bufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, readBufferSize)
			},
		},
	}
}

// FetchMetrics holds detailed timing metrics for a fetch operation.
type FetchMetrics struct {
	TotalDuration    time.Duration
	DNSLookup        time.Duration
	TCPConnect       time.Duration
	TLSHandshake     time.Duration
	TTFB             time.Duration // Time to first byte
	BodyRead         time.Duration
	JSONDecode       time.Duration
	RecordsFetched   int
	BytesTransferred int64
}

// String returns formatted metrics.
func (m *FetchMetrics) String() string {
	return fmt.Sprintf(
		"Fetch Metrics: total=%v dns=%v tcp=%v tls=%v ttfb=%v read=%v decode=%v records=%d bytes=%d",
		m.TotalDuration, m.DNSLookup, m.TCPConnect, m.TLSHandshake,
		m.TTFB, m.BodyRead, m.JSONDecode, m.RecordsFetched, m.BytesTransferred,
	)
}

// StreamingDailyRecord is a minimal struct for streaming JSON decode.
type StreamingDailyRecord struct {
	Date          string                   `json:"Date"`
	DayAirTmpAvg  *MinimalMeasurementValue `json:"DayAirTmpAvg,omitempty"`
	DayAsceEto    *MinimalMeasurementValue `json:"DayAsceEto,omitempty"`
	DayWindSpdAvg *MinimalMeasurementValue `json:"DayWindSpdAvg,omitempty"`
	DayRelHumAvg  *MinimalMeasurementValue `json:"DayRelHumAvg,omitempty"`
	DaySolRadAvg  *MinimalMeasurementValue `json:"DaySolRadAvg,omitempty"`
	DayPrecip     *MinimalMeasurementValue `json:"DayPrecip,omitempty"`
}

// StreamingProvider wraps records for streaming decode.
type StreamingProvider struct {
	Records []StreamingDailyRecord `json:"Records"`
}

// FetchDailyDataStreaming retrieves daily data with streaming JSON decode.
// This minimizes memory allocations compared to the standard FetchDailyData.
func (c *OptimizedClient) FetchDailyDataStreaming(stationID int, startDate, endDate string) ([]types.DailyRecord, *FetchMetrics, error) {
	metrics := &FetchMetrics{}
	start := time.Now()

	// Build URL
	params := url.Values{}
	params.Set("appKey", c.appKey)
	params.Set("targets", strconv.Itoa(stationID))
	params.Set("startDate", startDate)
	params.Set("endDate", endDate)
	params.Set("dataItems", "day-air-tmp-avg,day-asce-eto,day-wind-spd-avg,day-rel-hum-avg,day-sol-rad-avg,day-precip")
	params.Set("unitOfMeasure", "M")

	requestURL := fmt.Sprintf("%s?%s", c.baseURL, params.Encode())

	// Create request with context for cancellation
	ctx, cancel := context.WithTimeout(context.Background(), streamingTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", requestURL, nil)
	if err != nil {
		return nil, metrics, fmt.Errorf("failed to create request: %w", err)
	}

	// Accept gzip encoding
	req.Header.Set("Accept-Encoding", "gzip")

	// Execute request with detailed timing
	dialStart := time.Now()
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, metrics, fmt.Errorf("failed to fetch data: %w", err)
	}
	defer resp.Body.Close()

	// For now, we can't easily split DNS/TCP/TLS without custom DialContext
	// But we can measure TTFB
	metrics.DNSLookup = time.Since(dialStart) // Approximate
	metrics.TTFB = time.Since(start)

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, metrics, fmt.Errorf("API returned status %d: %s", resp.StatusCode, string(body))
	}

	// Stream decode with bufio for reduced syscalls
	readStart := time.Now()
	bufReader := bufio.NewReaderSize(resp.Body, readBufferSize)
	metrics.BodyRead = time.Since(readStart)

	decodeStart := time.Now()
	records, err := c.streamDecodeDaily(bufReader, uint16(stationID))
	if err != nil {
		return nil, metrics, fmt.Errorf("failed to decode: %w", err)
	}
	metrics.JSONDecode = time.Since(decodeStart)

	metrics.TotalDuration = time.Since(start)
	metrics.RecordsFetched = len(records)

	return records, metrics, nil
}

// streamDecodeDaily performs streaming JSON decode to minimize allocations.
func (c *OptimizedClient) streamDecodeDaily(r io.Reader, stationID uint16) ([]types.DailyRecord, error) {
	dec := json.NewDecoder(r)

	// Navigate to Data.Providers
	for dec.More() {
		token, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("decode token error: %w", err)
		}

		switch t := token.(type) {
		case string:
			if t == "Providers" {
				// Found providers array
				var providers []StreamingProvider
				if err := dec.Decode(&providers); err != nil {
					return nil, fmt.Errorf("failed to decode providers: %w", err)
				}

				// Convert streaming records to binary format
				var records []types.DailyRecord
				for _, provider := range providers {
					for _, sdr := range provider.Records {
						record := c.streamingToDailyRecord(sdr, stationID)
						if record.Timestamp > 0 { // Valid record
							records = append(records, record)
						}
					}
				}
				return records, nil
			}
		}
	}

	return nil, fmt.Errorf("Providers not found in response")
}

// streamingToDailyRecord converts streaming record to binary format.
func (c *OptimizedClient) streamingToDailyRecord(sdr StreamingDailyRecord, stationID uint16) types.DailyRecord {
	// Fast date parse
	year, month, day, ok := parseDateYYYYMMDD(sdr.Date)
	var ts uint32
	if ok {
		ts = daysSinceEpoch(year, month, day)
	} else {
		// Fallback to time.Parse
		date, err := time.Parse("2006-01-02", sdr.Date)
		if err != nil {
			return types.DailyRecord{}
		}
		ts = types.TimeToDaysSinceEpoch(date)
	}

	// Extract values directly
	var temp, et, wind, humidity, solar float64
	var qcFlags uint8

	if sdr.DayAirTmpAvg != nil {
		temp = sdr.DayAirTmpAvg.Value
		if sdr.DayAirTmpAvg.Qc != " " && sdr.DayAirTmpAvg.Qc != "" {
			qcFlags |= 0x01
		}
	}
	if sdr.DayAsceEto != nil {
		et = sdr.DayAsceEto.Value
		if sdr.DayAsceEto.Qc != " " && sdr.DayAsceEto.Qc != "" {
			qcFlags |= 0x02
		}
	}
	if sdr.DayWindSpdAvg != nil {
		wind = sdr.DayWindSpdAvg.Value
	}
	if sdr.DayRelHumAvg != nil {
		humidity = sdr.DayRelHumAvg.Value
	}
	if sdr.DaySolRadAvg != nil {
		solar = sdr.DaySolRadAvg.Value
	}

	return types.DailyRecord{
		Timestamp:      ts,
		StationID:      stationID,
		Temperature:    types.ScaleTemperature(temp),
		ET:             types.ScaleET(et),
		WindSpeed:      types.ScaleWindSpeed(wind),
		Humidity:       uint8(humidity),
		SolarRadiation: uint8(solar * 10),
		QCFlags:        qcFlags,
	}
}

// BatchFetchResult holds results from parallel fetches.
type BatchFetchResult struct {
	StationID uint16
	Records   []types.DailyRecord
	Metrics   *FetchMetrics
	Error     error
}

// FetchMultipleStations fetches data for multiple stations in parallel.
func (c *OptimizedClient) FetchMultipleStations(stationIDs []uint16, startDate, endDate string, workerPool int) []BatchFetchResult {
	if workerPool <= 0 {
		workerPool = 4 // Default
	}

	results := make([]BatchFetchResult, len(stationIDs))
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, workerPool)

	for i, stationID := range stationIDs {
		wg.Add(1)
		semaphore <- struct{}{} // Acquire

		go func(idx int, sid uint16) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release

			records, metrics, err := c.FetchDailyDataStreaming(int(sid), startDate, endDate)
			results[idx] = BatchFetchResult{
				StationID: sid,
				Records:   records,
				Metrics:   metrics,
				Error:     err,
			}
		}(i, stationID)
	}

	wg.Wait()
	return results
}
