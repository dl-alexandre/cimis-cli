package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dl-alexandre/cimis-tsdb/types"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestParseCIMISDate(t *testing.T) {
	tests := []struct {
		input   string
		wantErr bool
		want    time.Time
	}{
		{"2024-01-15", false, time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)},
		{"2023-12-31", false, time.Date(2023, 12, 31, 0, 0, 0, 0, time.UTC)},
		{"2024-02-29", false, time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)}, // leap year
		{"01/15/2024", false, time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)}, // legacy CLI input
		{"", true, time.Time{}},
		{"not-a-date", true, time.Time{}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseCIMISDate(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseCIMISDate(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && !got.Equal(tt.want) {
				t.Errorf("ParseCIMISDate(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestFormatCIMISDate(t *testing.T) {
	tests := []struct {
		input time.Time
		want  string
	}{
		{time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), "2024-01-15"},
		{time.Date(2023, 12, 31, 0, 0, 0, 0, time.UTC), "2023-12-31"},
		{time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC), "2024-02-29"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := FormatCIMISDate(tt.input)
			if got != tt.want {
				t.Errorf("FormatCIMISDate(%v) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestFormatCIMISDateRoundTrip(t *testing.T) {
	original := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	formatted := FormatCIMISDate(original)
	parsed, err := ParseCIMISDate(formatted)
	if err != nil {
		t.Fatalf("round-trip failed: %v", err)
	}
	if !parsed.Equal(original) {
		t.Errorf("round-trip: got %v, want %v", parsed, original)
	}
}

func TestNormalizeCIMISDate(t *testing.T) {
	tests := map[string]string{
		"2024-06-15": "2024-06-15",
		"06/15/2024": "2024-06-15",
		"bad-date":   "bad-date",
	}

	for input, want := range tests {
		if got := NormalizeCIMISDate(input); got != want {
			t.Errorf("NormalizeCIMISDate(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestBuildCIMISURLValidation(t *testing.T) {
	if _, err := buildCIMISURL("://bad-url", "/StationWeb/GetAllStations", nil); err == nil {
		t.Fatal("expected parse error for malformed base URL")
	}

	if _, err := buildCIMISURL("et.water.ca.gov", "/StationWeb/GetAllStations", nil); err == nil {
		t.Fatal("expected error for base URL without scheme and host")
	}

	got, err := buildCIMISURL("https://example.test/base/", "/StationWeb/GetAllStations", nil)
	if err != nil {
		t.Fatalf("buildCIMISURL() error = %v", err)
	}
	if got != "https://example.test/base/StationWeb/GetAllStations" {
		t.Errorf("buildCIMISURL() = %q", got)
	}
}

func TestNewCIMISRequestWithoutAppKey(t *testing.T) {
	req, requestURL, err := newCIMISRequest(t.Context(), "https://example.test", "/StationWeb/GetAllStations", nil, "")
	if err != nil {
		t.Fatalf("newCIMISRequest() error = %v", err)
	}
	if requestURL != "https://example.test/StationWeb/GetAllStations" {
		t.Fatalf("requestURL = %q", requestURL)
	}
	if got := req.Header.Get(SubscriptionKeyHeader); got != "" {
		t.Fatalf("%s = %q, want empty", SubscriptionKeyHeader, got)
	}
	if got := req.Header.Get("Accept"); got != "application/json" {
		t.Fatalf("Accept = %q, want application/json", got)
	}
}

func TestNewCIMISRequestWithAppKey(t *testing.T) {
	req, requestURL, err := newCIMISRequest(t.Context(), "https://example.test", "/StationWeb/GetAllStations", nil, "test-key")
	if err != nil {
		t.Fatalf("newCIMISRequest() error = %v", err)
	}
	if requestURL != "https://example.test/StationWeb/GetAllStations" {
		t.Fatalf("requestURL = %q", requestURL)
	}
	if got := req.Header.Get(SubscriptionKeyHeader); got != "test-key" {
		t.Fatalf("%s = %q, want test-key", SubscriptionKeyHeader, got)
	}
}

func TestNewCIMISRequestConstructorError(t *testing.T) {
	original := newHTTPRequest
	t.Cleanup(func() {
		newHTTPRequest = original
	})
	newHTTPRequest = func(context.Context, string, string, io.Reader) (*http.Request, error) {
		return nil, errors.New("request failed")
	}

	if _, _, err := newCIMISRequest(t.Context(), "https://example.test", "/StationWeb/GetAllStations", nil, "test-key"); err == nil {
		t.Fatal("expected request constructor error")
	}
}

func TestParseMeasurementValue(t *testing.T) {
	tests := []struct {
		name string
		mv   *MeasurementValue
		want float64
	}{
		{"nil", nil, 0},
		{"valid float", &MeasurementValue{Value: "23.5"}, 23.5},
		{"zero", &MeasurementValue{Value: "0"}, 0},
		{"negative", &MeasurementValue{Value: "-5.2"}, -5.2},
		{"empty string", &MeasurementValue{Value: ""}, 0},
		{"non-numeric", &MeasurementValue{Value: "abc"}, 0},
		{"with whitespace", &MeasurementValue{Value: " "}, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseMeasurementValue(tt.mv)
			if got != tt.want {
				t.Errorf("ParseMeasurementValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHasQCFlag(t *testing.T) {
	tests := []struct {
		name string
		mv   *MeasurementValue
		want bool
	}{
		{"nil", nil, false},
		{"space (good data)", &MeasurementValue{Qc: " "}, false},
		{"empty (good data)", &MeasurementValue{Qc: ""}, false},
		{"R flag", &MeasurementValue{Qc: "R"}, true},
		{"Y flag", &MeasurementValue{Qc: "Y"}, true},
		{"any non-space", &MeasurementValue{Qc: "X"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := HasQCFlag(tt.mv)
			if got != tt.want {
				t.Errorf("HasQCFlag() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseDateYYYYMMDD(t *testing.T) {
	tests := []struct {
		input                        string
		wantYear, wantMonth, wantDay int
		wantOK                       bool
	}{
		{"2024-01-15", 2024, 1, 15, true},
		{"1985-01-01", 1985, 1, 1, true},
		{"2099-12-31", 2099, 12, 31, true},
		{"", 0, 0, 0, false},
		{"short", 0, 0, 0, false},
		{"2024/01/15", 0, 0, 0, false}, // wrong separator
		{"20240115xx", 0, 0, 0, false}, // wrong length ok but wrong format
		{"1984-01-01", 0, 0, 0, false}, // before epoch
		{"2101-01-01", 0, 0, 0, false}, // after 2100
		{"2024-13-01", 0, 0, 0, false}, // invalid month
		{"2024-00-01", 0, 0, 0, false}, // month 0
		{"2024-01-00", 0, 0, 0, false}, // day 0
		{"2024-01-32", 0, 0, 0, false}, // day 32
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			year, month, day, ok := parseDateYYYYMMDD(tt.input)
			if ok != tt.wantOK {
				t.Errorf("parseDateYYYYMMDD(%q) ok = %v, want %v", tt.input, ok, tt.wantOK)
				return
			}
			if ok {
				if year != tt.wantYear || month != tt.wantMonth || day != tt.wantDay {
					t.Errorf("parseDateYYYYMMDD(%q) = (%d, %d, %d), want (%d, %d, %d)",
						tt.input, year, month, day, tt.wantYear, tt.wantMonth, tt.wantDay)
				}
			}
		})
	}
}

func TestDaysSinceEpoch(t *testing.T) {
	// 1985-01-01 should be day 0
	d0 := daysSinceEpoch(1985, 1, 1)
	if d0 != 0 {
		t.Errorf("daysSinceEpoch(1985, 1, 1) = %d, want 0", d0)
	}

	// 1985-01-02 should be day 1
	d1 := daysSinceEpoch(1985, 1, 2)
	if d1 != 1 {
		t.Errorf("daysSinceEpoch(1985, 1, 2) = %d, want 1", d1)
	}

	// Days should increase monotonically
	prev := daysSinceEpoch(2020, 1, 1)
	for m := 1; m <= 12; m++ {
		cur := daysSinceEpoch(2020, m, 15)
		if cur <= prev && m > 1 {
			t.Errorf("daysSinceEpoch not monotonic: month %d (%d) <= previous (%d)", m, cur, prev)
		}
		prev = cur
	}
}

func TestConvertDailyToRecords(t *testing.T) {
	apiRecords := []*DailyDataRecord{
		{
			Date:          "2024-06-15",
			DayAirTmpAvg:  &MeasurementValue{Value: "25.3", Qc: " "},
			DayAsceEto:    &MeasurementValue{Value: "5.2", Qc: " "},
			DayWindSpdAvg: &MeasurementValue{Value: "3.1", Qc: " "},
			DayRelHumAvg:  &MeasurementValue{Value: "65", Qc: " "},
			DaySolRadAvg:  &MeasurementValue{Value: "2.5", Qc: " "},
		},
	}

	records := ConvertDailyToRecords(apiRecords, 2)

	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}

	r := records[0]
	if r.StationID != 2 {
		t.Errorf("StationID = %d, want 2", r.StationID)
	}
	if r.Timestamp == 0 {
		t.Error("Timestamp should be non-zero for 2024 date")
	}
	if r.Humidity != 65 {
		t.Errorf("Humidity = %d, want 65", r.Humidity)
	}
	if r.QCFlags != 0 {
		t.Errorf("QCFlags = %d, want 0 (all good data)", r.QCFlags)
	}
}

func TestConvertDailyToRecordsQCFlags(t *testing.T) {
	apiRecords := []*DailyDataRecord{
		{
			Date:         "2024-06-15",
			DayAirTmpAvg: &MeasurementValue{Value: "25.0", Qc: "R"}, // flagged
			DayAsceEto:   &MeasurementValue{Value: "5.0", Qc: "Y"},  // flagged
		},
	}

	records := ConvertDailyToRecords(apiRecords, 1)
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}

	if records[0].QCFlags&0x01 == 0 {
		t.Error("expected temperature QC flag (0x01) to be set")
	}
	if records[0].QCFlags&0x02 == 0 {
		t.Error("expected ET QC flag (0x02) to be set")
	}
}

func TestConvertDailyToRecordsSkipsInvalidDates(t *testing.T) {
	apiRecords := []*DailyDataRecord{
		{Date: "invalid-date"},
		{Date: "2024-06-15", DayAirTmpAvg: &MeasurementValue{Value: "20.0", Qc: " "}},
	}

	records := ConvertDailyToRecords(apiRecords, 1)
	if len(records) != 1 {
		t.Errorf("got %d records, want 1 (invalid date should be skipped)", len(records))
	}
}

func TestConvertDailyToRecordsNilMeasurements(t *testing.T) {
	apiRecords := []*DailyDataRecord{
		{Date: "2024-06-15"}, // all measurements nil
	}

	records := ConvertDailyToRecords(apiRecords, 1)
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}

	r := records[0]
	if r.Humidity != 0 {
		t.Errorf("Humidity = %d, want 0 for nil measurement", r.Humidity)
	}
	if r.QCFlags != 0 {
		t.Errorf("QCFlags = %d, want 0 for nil measurements", r.QCFlags)
	}
}

func TestConvertDailyToRecordsFast(t *testing.T) {
	apiRecords := []*DailyDataRecord{
		{
			Date:         "2024-06-15",
			DayAirTmpAvg: &MeasurementValue{Value: "25.3", Qc: " "},
			DayAsceEto:   &MeasurementValue{Value: "5.2", Qc: " "},
		},
	}

	standard := ConvertDailyToRecords(apiRecords, 2)
	fast := ConvertDailyToRecordsFast(apiRecords, 2)

	if len(standard) != len(fast) {
		t.Fatalf("record count mismatch: standard=%d, fast=%d", len(standard), len(fast))
	}

	if standard[0].Timestamp != fast[0].Timestamp {
		t.Errorf("timestamp mismatch: standard=%d, fast=%d", standard[0].Timestamp, fast[0].Timestamp)
	}
	if standard[0].Temperature != fast[0].Temperature {
		t.Errorf("temperature mismatch: standard=%d, fast=%d", standard[0].Temperature, fast[0].Temperature)
	}
	if standard[0].QCFlags != fast[0].QCFlags {
		t.Errorf("QCFlags mismatch: standard=%d, fast=%d", standard[0].QCFlags, fast[0].QCFlags)
	}
}

func TestConvertHourlyToRecords(t *testing.T) {
	apiRecords := []*HourlyDataRecord{
		{
			Date:      "2024-06-15",
			Hour:      "14:00",
			HlyAirTmp: &MeasurementValue{Value: "28.5", Qc: " "},
			HlyRelHum: &MeasurementValue{Value: "55", Qc: " "},
		},
	}

	records := ConvertHourlyToRecords(apiRecords, 5)

	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}

	r := records[0]
	if r.StationID != 5 {
		t.Errorf("StationID = %d, want 5", r.StationID)
	}
	if r.Humidity != 55 {
		t.Errorf("Humidity = %d, want 55", r.Humidity)
	}
}

func TestConvertHourlyToRecordsHourParsing(t *testing.T) {
	rec1 := []*HourlyDataRecord{{Date: "2024-06-15", Hour: "00:00"}}
	rec2 := []*HourlyDataRecord{{Date: "2024-06-15", Hour: "23:00"}}

	r1 := ConvertHourlyToRecords(rec1, 1)
	r2 := ConvertHourlyToRecords(rec2, 1)

	if len(r1) != 1 || len(r2) != 1 {
		t.Fatal("expected 1 record each")
	}

	// Hour 23 should have a later timestamp than hour 0 on the same day
	if r2[0].Timestamp <= r1[0].Timestamp {
		t.Errorf("hour 23 timestamp (%d) should be > hour 0 timestamp (%d)",
			r2[0].Timestamp, r1[0].Timestamp)
	}
}

func TestConvertHourlyToRecordsSkipsInvalidDateAndHandlesShortHour(t *testing.T) {
	apiRecords := []*HourlyDataRecord{
		{Date: "bad-date", Hour: "12:00", HlyAirTmp: &MeasurementValue{Value: "30.0", Qc: "R"}},
		{Date: "2024-06-15", Hour: "1", HlyAirTmp: &MeasurementValue{Value: "28.5", Qc: "R"}},
	}

	records := ConvertHourlyToRecords(apiRecords, 5)
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	if records[0].StationID != 5 {
		t.Fatalf("StationID = %d, want 5", records[0].StationID)
	}
	if records[0].QCFlags&0x01 == 0 {
		t.Fatalf("expected hourly air temperature QC flag, got %d", records[0].QCFlags)
	}
}

func TestFetchDailyDataHTTP(t *testing.T) {
	response := APIResponse{}
	response.Data.Providers = []Provider{
		{
			Name: "CIMIS",
			Records: []*DailyDataRecord{
				{
					Date:         "2024-06-15",
					Station:      "2",
					DayAirTmpAvg: &MeasurementValue{Value: "25.0", Qc: " ", Unit: "(C)"},
				},
				{
					Date:         "2024-06-16",
					Station:      "2",
					DayAirTmpAvg: &MeasurementValue{Value: "26.0", Qc: " ", Unit: "(C)"},
				},
			},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/StationWeb/GetDataByStationNumber" {
			t.Errorf("path = %q, want /StationWeb/GetDataByStationNumber", r.URL.Path)
		}
		if got := r.Header.Get(SubscriptionKeyHeader); got != "test-key" {
			t.Errorf("%s = %q, want test-key", SubscriptionKeyHeader, got)
		}
		if got := r.Header.Get("Accept"); got != "application/json" {
			t.Errorf("Accept = %q, want application/json", got)
		}

		// Verify expected query params
		q := r.URL.Query()
		if q.Get("stationNbrs") != "2" {
			t.Errorf("stationNbrs = %q, want 2", q.Get("stationNbrs"))
		}
		if q.Get("startDate") != "2024-06-15" {
			t.Errorf("startDate = %q, want 2024-06-15", q.Get("startDate"))
		}
		if q.Get("endDate") != "2024-06-16" {
			t.Errorf("endDate = %q, want 2024-06-16", q.Get("endDate"))
		}
		if q.Get("isHourly") != "false" {
			t.Errorf("isHourly = %q, want false", q.Get("isHourly"))
		}
		if q.Get("unitOfMeasure") != "M" {
			t.Errorf("unitOfMeasure = %q, want M", q.Get("unitOfMeasure"))
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := NewClient("test-key")
	client.baseURL = server.URL

	records, err := client.FetchDailyData(2, "06/15/2024", "06/16/2024")
	if err != nil {
		t.Fatalf("FetchDailyData() error = %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("got %d records, want 2", len(records))
	}

	if records[0].Date != "2024-06-15" {
		t.Errorf("record[0].Date = %q, want 2024-06-15", records[0].Date)
	}
}

func TestFetchDailyDataHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "Internal Server Error")
	}))
	defer server.Close()

	client := NewClient("test-key")
	client.baseURL = server.URL

	_, err := client.FetchDailyData(2, "06/15/2024", "06/16/2024")
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestGetJSONRequestAndTransportErrors(t *testing.T) {
	t.Run("request build error", func(t *testing.T) {
		client := NewClient("test-key")
		client.SetBaseURL("://bad-url")

		var target StationsResponse
		if err := client.getJSON(allStationsPath, nil, &target); err == nil {
			t.Fatal("expected request build error")
		}
	})

	t.Run("transport error", func(t *testing.T) {
		client := NewClient("test-key")
		client.SetHTTPClient(&http.Client{
			Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return nil, errors.New("network down")
			}),
		})

		var target StationsResponse
		if err := client.getJSON(allStationsPath, nil, &target); err == nil {
			t.Fatal("expected transport error")
		}
	})
}

func TestAPIErrorHints(t *testing.T) {
	tests := []struct {
		status int
		want   string
	}{
		{http.StatusForbidden, "CIMIS_APP_KEY"},
		{http.StatusTooManyRequests, "rate limited"},
		{http.StatusBadGateway, "retry later"},
	}

	for _, tt := range tests {
		err := apiError(tt.status, "https://example.test", []byte("body"))
		if err == nil {
			t.Fatal("apiError returned nil")
		}
		if !strings.Contains(err.Error(), tt.want) {
			t.Fatalf("apiError(%d) = %q, want to contain %q", tt.status, err.Error(), tt.want)
		}
	}
}

func TestFetchDailyDataMalformedJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{invalid json`)
	}))
	defer server.Close()

	client := NewClient("test-key")
	client.baseURL = server.URL

	_, err := client.FetchDailyData(2, "06/15/2024", "06/16/2024")
	if err == nil {
		t.Fatal("expected error for malformed JSON")
	}
}

func TestFetchDailyDataEmptyProviders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"Data":{"Providers":[]}}`)
	}))
	defer server.Close()

	client := NewClient("test-key")
	client.baseURL = server.URL

	records, err := client.FetchDailyData(2, "06/15/2024", "06/16/2024")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(records) != 0 {
		t.Errorf("got %d records, want 0", len(records))
	}
}

func TestFetchHourlyDataHTTP(t *testing.T) {
	response := HourlyAPIResponse{}
	response.Data.Providers = []HourlyProvider{
		{
			Name: "CIMIS",
			Records: []*HourlyDataRecord{
				{Date: "2024-06-15", Hour: "14:00", HlyAirTmp: &MeasurementValue{Value: "28.0", Qc: " "}},
			},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/StationWeb/GetDataByStationNumber" {
			t.Errorf("path = %q, want /StationWeb/GetDataByStationNumber", r.URL.Path)
		}
		if got := r.Header.Get(SubscriptionKeyHeader); got != "test-key" {
			t.Errorf("%s = %q, want test-key", SubscriptionKeyHeader, got)
		}
		if got := r.URL.Query().Get("isHourly"); got != "true" {
			t.Errorf("isHourly = %q, want true", got)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := NewClient("test-key")
	client.baseURL = server.URL

	records, err := client.FetchHourlyData(2, "06/15/2024", "06/15/2024")
	if err != nil {
		t.Fatalf("FetchHourlyData() error = %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	if records[0].Hour != "14:00" {
		t.Errorf("Hour = %q, want 14:00", records[0].Hour)
	}
}

func TestDataEndpointHTTPErrorBranches(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "upstream down", http.StatusBadGateway)
	}))
	defer server.Close()

	client := NewClient("test-key")
	client.SetBaseURL(server.URL)

	tests := []struct {
		name string
		call func() error
	}{
		{"FetchHourlyData", func() error {
			_, err := client.FetchHourlyData(2, "2024-06-15", "2024-06-16")
			return err
		}},
		{"FetchHourlyDataByStationZipCodes", func() error {
			_, err := client.FetchHourlyDataByStationZipCodes([]string{"93624"}, "2024-06-15", "2024-06-16")
			return err
		}},
		{"FetchDailyDataBySpatialZipCodes", func() error {
			_, err := client.FetchDailyDataBySpatialZipCodes([]string{"95974"}, "2024-06-15", "2024-06-16")
			return err
		}},
		{"FetchDailyDataBySpatialCoordinates", func() error {
			_, err := client.FetchDailyDataBySpatialCoordinates([]Coordinate{{Lat: 34.99, Lng: -118.34}}, "2024-06-15", "2024-06-16")
			return err
		}},
		{"FetchDailyDataBySpatialAddresses", func() error {
			_, err := client.FetchDailyDataBySpatialAddresses([]SpatialAddress{{Name: "Capitol", Address: "Sacramento, CA"}}, "2024-06-15", "2024-06-16")
			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call(); err == nil {
				t.Fatal("expected endpoint error")
			}
		})
	}
}

func TestConvertMinimalDailyToRecords(t *testing.T) {
	minRecords := []MinimalDailyRecord{
		{
			Date:          "2024-06-15",
			DayAirTmpAvg:  &MinimalMeasurementValue{Value: 25.3, Qc: " "},
			DayAsceEto:    &MinimalMeasurementValue{Value: 5.2, Qc: " "},
			DayWindSpdAvg: &MinimalMeasurementValue{Value: 3.1, Qc: " "},
			DayRelHumAvg:  &MinimalMeasurementValue{Value: 65.0, Qc: " "},
			DaySolRadAvg:  &MinimalMeasurementValue{Value: 2.5, Qc: " "},
		},
	}

	records := ConvertMinimalDailyToRecords(minRecords, 2)

	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}

	r := records[0]
	if r.StationID != 2 {
		t.Errorf("StationID = %d, want 2", r.StationID)
	}
	if r.Humidity != 65 {
		t.Errorf("Humidity = %d, want 65", r.Humidity)
	}
	if r.QCFlags != 0 {
		t.Errorf("QCFlags = %d, want 0", r.QCFlags)
	}
}

func TestConvertMinimalDailyToRecordsQCFlags(t *testing.T) {
	minRecords := []MinimalDailyRecord{
		{
			Date:         "2024-06-15",
			DayAirTmpAvg: &MinimalMeasurementValue{Value: 25.0, Qc: "R"},
			DayAsceEto:   &MinimalMeasurementValue{Value: 5.0, Qc: "Y"},
		},
	}

	records := ConvertMinimalDailyToRecords(minRecords, 1)
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}

	if records[0].QCFlags&0x01 == 0 {
		t.Error("expected temperature QC flag (0x01) to be set")
	}
	if records[0].QCFlags&0x02 == 0 {
		t.Error("expected ET QC flag (0x02) to be set")
	}
}

func TestConvertMinimalDailyToRecordsSkipsInvalidDateAndHandlesNilMeasurements(t *testing.T) {
	minRecords := []MinimalDailyRecord{
		{Date: "bad-date", DayAirTmpAvg: &MinimalMeasurementValue{Value: 25.0, Qc: "R"}},
		{Date: "2024-06-15"},
		{Date: "2101-01-02", DayAirTmpAvg: &MinimalMeasurementValue{Value: 24.5, Qc: " "}},
	}

	records := ConvertMinimalDailyToRecords(minRecords, 9)
	if len(records) != 2 {
		t.Fatalf("got %d records, want 2", len(records))
	}
	if records[0].StationID != 9 {
		t.Fatalf("StationID = %d, want 9", records[0].StationID)
	}
	if records[0].Temperature != 0 || records[0].QCFlags != 0 {
		t.Fatalf("nil measurements produced record %+v", records[0])
	}
	if records[1].Temperature == 0 {
		t.Fatalf("fallback date record was not populated: %+v", records[1])
	}
}

func TestFetchDailyDataByStationZipCodesHTTP(t *testing.T) {
	response := APIResponse{}
	response.Data.Providers = []Provider{
		{
			Name: "CIMIS",
			Records: []*DailyDataRecord{
				{Date: "2024-06-15", ZipCodes: "93624", DayAsceEto: &MeasurementValue{Value: "5.0", Qc: " "}},
			},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/StationWeb/GetDataByStationZipCodes" {
			t.Errorf("path = %q, want /StationWeb/GetDataByStationZipCodes", r.URL.Path)
		}
		q := r.URL.Query()
		if q.Get("zipCodes") != "93624,94503" {
			t.Errorf("zipCodes = %q, want 93624,94503", q.Get("zipCodes"))
		}
		if q.Get("isHourly") != "false" {
			t.Errorf("isHourly = %q, want false", q.Get("isHourly"))
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := NewClient("test-key")
	client.SetBaseURL(server.URL)

	records, err := client.FetchDailyDataByStationZipCodes([]string{"93624", "94503"}, "2024-06-15", "2024-06-16")
	if err != nil {
		t.Fatalf("FetchDailyDataByStationZipCodes() error = %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
}

func TestFetchHourlyDataByStationZipCodesHTTP(t *testing.T) {
	response := HourlyAPIResponse{}
	response.Data.Providers = []HourlyProvider{
		{
			Name: "CIMIS",
			Records: []*HourlyDataRecord{
				{Date: "2024-06-15", Hour: "0100", ZipCodes: "93624", HlyAirTmp: &MeasurementValue{Value: "28.0", Qc: " "}},
			},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/StationWeb/GetDataByStationZipCodes" {
			t.Errorf("path = %q, want /StationWeb/GetDataByStationZipCodes", r.URL.Path)
		}
		q := r.URL.Query()
		if q.Get("zipCodes") != "93624" {
			t.Errorf("zipCodes = %q, want 93624", q.Get("zipCodes"))
		}
		if q.Get("isHourly") != "true" {
			t.Errorf("isHourly = %q, want true", q.Get("isHourly"))
		}
		if q.Get("dataItems") != HourlyDataItems {
			t.Errorf("dataItems = %q, want %q", q.Get("dataItems"), HourlyDataItems)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := NewClient("test-key")
	client.SetBaseURL(server.URL)

	records, err := client.FetchHourlyDataByStationZipCodes([]string{"93624"}, "2024-06-15", "2024-06-16")
	if err != nil {
		t.Fatalf("FetchHourlyDataByStationZipCodes() error = %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
}

func TestFetchDailyDataBySpatialZipCodesUsesSpatialItems(t *testing.T) {
	response := APIResponse{}
	response.Data.Providers = []Provider{
		{
			Name:    "CIMIS",
			Type:    "spatial",
			Records: []*DailyDataRecord{{Date: "2024-06-15", ZipCodes: "95974"}},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/SpatialWeb/GetDataBySpatialZipCodes" {
			t.Errorf("path = %q, want /SpatialWeb/GetDataBySpatialZipCodes", r.URL.Path)
		}
		if got := r.URL.Query().Get("dataItems"); got != SpatialZipDataItems {
			t.Errorf("dataItems = %q, want %q", got, SpatialZipDataItems)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := NewClient("test-key")
	client.SetBaseURL(server.URL)

	records, err := client.FetchDailyDataBySpatialZipCodes([]string{"95974"}, "2024-06-15", "2024-06-16")
	if err != nil {
		t.Fatalf("FetchDailyDataBySpatialZipCodes() error = %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
}

func TestFetchDailyDataBySpatialCoordinatesHTTP(t *testing.T) {
	response := APIResponse{}
	response.Data.Providers = []Provider{
		{
			Name:    "CIMIS",
			Type:    "spatial",
			Records: []*DailyDataRecord{{Date: "2024-06-15", Coordinate: "lat=34.99,lng=-118.34"}},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/SpatialWeb/GetDataBySpatialCoordinates" {
			t.Errorf("path = %q, want /SpatialWeb/GetDataBySpatialCoordinates", r.URL.Path)
		}
		q := r.URL.Query()
		if q.Get("coordinates") != "lat=34.99,lng=-118.34;lat=36.45,lng=-118.16" {
			t.Errorf("coordinates = %q", q.Get("coordinates"))
		}
		if q.Get("dataItems") != SpatialPointDataItems {
			t.Errorf("dataItems = %q, want %q", q.Get("dataItems"), SpatialPointDataItems)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := NewClient("test-key")
	client.SetBaseURL(server.URL)

	records, err := client.FetchDailyDataBySpatialCoordinates(
		[]Coordinate{{Lat: 34.99, Lng: -118.34}, {Lat: 36.45, Lng: -118.16}},
		"2024-06-15",
		"2024-06-16",
	)
	if err != nil {
		t.Fatalf("FetchDailyDataBySpatialCoordinates() error = %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
}

func TestFetchDailyDataByGeoStationZipCodesHTTP(t *testing.T) {
	response := APIResponse{}
	response.Data.Providers = []Provider{
		{Name: "CIMIS", Type: "spatial", Records: []*DailyDataRecord{{Date: "2024-06-15", ZipCodes: "94503"}}},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/GeoStationWeb/GetDataByGeoStationZipCodes" {
			t.Errorf("path = %q, want /GeoStationWeb/GetDataByGeoStationZipCodes", r.URL.Path)
		}
		q := r.URL.Query()
		if q.Get("zipCodes") != "94503,95667" {
			t.Errorf("zipCodes = %q, want 94503,95667", q.Get("zipCodes"))
		}
		if q.Get("prefer") != "SCS" {
			t.Errorf("prefer = %q, want SCS", q.Get("prefer"))
		}
		if q.Get("dataItems") != SpatialZipDataItems {
			t.Errorf("dataItems = %q, want %q", q.Get("dataItems"), SpatialZipDataItems)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := NewClient("test-key")
	client.SetBaseURL(server.URL)

	records, err := client.FetchDailyDataByGeoStationZipCodes([]string{"94503", "95667"}, "2024-06-15", "2024-06-16", "SCS")
	if err != nil {
		t.Fatalf("FetchDailyDataByGeoStationZipCodes() error = %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
}

func TestFetchDailyDataBySpatialAddressesHTTP(t *testing.T) {
	response := APIResponse{}
	response.Data.Providers = []Provider{
		{Name: "CIMIS", Type: "spatial", Records: []*DailyDataRecord{{Date: "2024-06-15", Address: "1315 10th Street Sacramento, CA"}}},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/SpatialWeb/GetDataByAddresses" {
			t.Errorf("path = %q, want /SpatialWeb/GetDataByAddresses", r.URL.Path)
		}
		q := r.URL.Query()
		wantAddresses := "addr-name=State Capitol,addr=1315 10th Street Sacramento, CA;addr-name=Theatre,addr=6925 Hollywood Boulevard, Los Angeles, CA"
		if q.Get("addresses") != wantAddresses {
			t.Errorf("addresses = %q, want %q", q.Get("addresses"), wantAddresses)
		}
		if q.Get("dataItems") != SpatialPointDataItems {
			t.Errorf("dataItems = %q, want %q", q.Get("dataItems"), SpatialPointDataItems)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := NewClient("test-key")
	client.SetBaseURL(server.URL)

	records, err := client.FetchDailyDataBySpatialAddresses(
		[]SpatialAddress{
			{Name: "State Capitol", Address: "1315 10th Street Sacramento, CA"},
			{Name: "Theatre", Address: "6925 Hollywood Boulevard, Los Angeles, CA"},
		},
		"2024-06-15",
		"2024-06-16",
	)
	if err != nil {
		t.Fatalf("FetchDailyDataBySpatialAddresses() error = %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
}

func TestFetchAllStationsHTTP(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/StationWeb/GetAllStations" {
			t.Errorf("path = %q, want /StationWeb/GetAllStations", r.URL.Path)
		}
		if got := r.Header.Get(SubscriptionKeyHeader); got != "test-key" {
			t.Errorf("%s = %q, want test-key", SubscriptionKeyHeader, got)
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"Stations":[{"StationNbr":"2","Name":"FivePoints","ZipCodes":["93624"]}]}`)
	}))
	defer server.Close()

	client := NewClient("test-key")
	client.SetBaseURL(server.URL)

	stations, err := client.FetchAllStations()
	if err != nil {
		t.Fatalf("FetchAllStations() error = %v", err)
	}
	if len(stations) != 1 {
		t.Fatalf("got %d stations, want 1", len(stations))
	}
	if stations[0].StationNbr != "2" {
		t.Errorf("StationNbr = %q, want 2", stations[0].StationNbr)
	}
}

func TestStationMetadataEndpointsHTTP(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/StationWeb/GetStationByStationNumber":
			if r.URL.Query().Get("stationNbr") != "2" {
				t.Errorf("stationNbr = %q, want 2", r.URL.Query().Get("stationNbr"))
			}
			fmt.Fprint(w, `{"Stations":[{"StationNbr":"2","Name":"FivePoints"}]}`)
		case "/StationWeb/GetAllStationsZipCodes":
			fmt.Fprint(w, `{"ZipCodes":[{"StationNbr":2,"ZipCode":"93624","IsActive":"True"}]}`)
		case "/StationWeb/GetStationZipCodeInfoByZipCode":
			if r.URL.Query().Get("zipCode") != "93624" {
				t.Errorf("zipCode = %q, want 93624", r.URL.Query().Get("zipCode"))
			}
			fmt.Fprint(w, `{"ZipCodes":[{"StationNbr":2,"ZipCode":"93624","IsActive":"True"}]}`)
		case "/SpatialWeb/GetAllSpatialZipCodes":
			fmt.Fprint(w, `{"ZipCodes":[{"ZipCode":"95974","IsActive":"True"}]}`)
		case "/SpatialWeb/GetSpatialZipCodeInfoByZipCode":
			if r.URL.Query().Get("zipCode") != "95974" {
				t.Errorf("zipCode = %q, want 95974", r.URL.Query().Get("zipCode"))
			}
			fmt.Fprint(w, `{"ZipCodes":[{"ZipCode":"95974","IsActive":"True"}]}`)
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := NewClient("test-key")
	client.SetBaseURL(server.URL)

	stations, err := client.FetchStation(2)
	if err != nil {
		t.Fatalf("FetchStation() error = %v", err)
	}
	if len(stations) != 1 || stations[0].StationNbr != "2" {
		t.Fatalf("FetchStation() = %+v", stations)
	}

	stationZips, err := client.FetchAllStationZipCodes()
	if err != nil {
		t.Fatalf("FetchAllStationZipCodes() error = %v", err)
	}
	if len(stationZips) != 1 || stationZips[0].StationNbr != 2 {
		t.Fatalf("FetchAllStationZipCodes() = %+v", stationZips)
	}

	stationZip, err := client.FetchStationZipCodeInfo("93624")
	if err != nil {
		t.Fatalf("FetchStationZipCodeInfo() error = %v", err)
	}
	if len(stationZip) != 1 || stationZip[0].ZipCode != "93624" {
		t.Fatalf("FetchStationZipCodeInfo() = %+v", stationZip)
	}

	spatialZips, err := client.FetchAllSpatialZipCodes()
	if err != nil {
		t.Fatalf("FetchAllSpatialZipCodes() error = %v", err)
	}
	if len(spatialZips) != 1 || spatialZips[0].ZipCode != "95974" {
		t.Fatalf("FetchAllSpatialZipCodes() = %+v", spatialZips)
	}

	spatialZip, err := client.FetchSpatialZipCodeInfo("95974")
	if err != nil {
		t.Fatalf("FetchSpatialZipCodeInfo() error = %v", err)
	}
	if len(spatialZip) != 1 || spatialZip[0].ZipCode != "95974" {
		t.Fatalf("FetchSpatialZipCodeInfo() = %+v", spatialZip)
	}
}

func TestStationMetadataEndpointsHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
	}))
	defer server.Close()

	client := NewClient("test-key")
	client.SetBaseURL(server.URL)

	tests := []struct {
		name string
		call func() error
	}{
		{"FetchAllStations", func() error {
			_, err := client.FetchAllStations()
			return err
		}},
		{"FetchStation", func() error {
			_, err := client.FetchStation(2)
			return err
		}},
		{"FetchAllStationZipCodes", func() error {
			_, err := client.FetchAllStationZipCodes()
			return err
		}},
		{"FetchStationZipCodeInfo", func() error {
			_, err := client.FetchStationZipCodeInfo("93624")
			return err
		}},
		{"FetchAllSpatialZipCodes", func() error {
			_, err := client.FetchAllSpatialZipCodes()
			return err
		}},
		{"FetchSpatialZipCodeInfo", func() error {
			_, err := client.FetchSpatialZipCodeInfo("95974")
			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call(); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestFetchDailyDataStreamingHTTP(t *testing.T) {
	response := `{"Data":{"Providers":[{"Name":"CIMIS","Records":[{"Date":"2024-06-15","DayAirTmpAvg":{"Value":"25.0","Qc":" "},"DayAsceEto":{"Value":"5.0","Qc":" "}}]}]}}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/StationWeb/GetDataByStationNumber" {
			t.Errorf("path = %q, want /StationWeb/GetDataByStationNumber", r.URL.Path)
		}
		if got := r.Header.Get(SubscriptionKeyHeader); got != "test-key" {
			t.Errorf("%s = %q, want test-key", SubscriptionKeyHeader, got)
		}
		q := r.URL.Query()
		if q.Get("stationNbrs") != "2" {
			t.Errorf("stationNbrs = %q, want 2", q.Get("stationNbrs"))
		}
		if q.Get("startDate") != "2024-06-15" {
			t.Errorf("startDate = %q, want 2024-06-15", q.Get("startDate"))
		}
		if q.Get("isHourly") != "false" {
			t.Errorf("isHourly = %q, want false", q.Get("isHourly"))
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, response)
	}))
	defer server.Close()

	client := NewOptimizedClient("test-key")
	client.SetBaseURL(server.URL)

	records, _, err := client.FetchDailyDataStreaming(2, "06/15/2024", "06/16/2024")
	if err != nil {
		t.Fatalf("FetchDailyDataStreaming() error = %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	if records[0].StationID != 2 {
		t.Errorf("StationID = %d, want 2", records[0].StationID)
	}
}

func TestFetchDailyDataStreamingErrors(t *testing.T) {
	t.Run("request build error", func(t *testing.T) {
		client := NewOptimizedClient("test-key")
		client.SetBaseURL("://bad-url")

		if _, _, err := client.FetchDailyDataStreaming(2, "2024-06-15", "2024-06-16"); err == nil {
			t.Fatal("expected request build error")
		}
	})

	t.Run("transport error", func(t *testing.T) {
		client := NewOptimizedClient("test-key")
		client.httpClient = &http.Client{
			Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return nil, errors.New("network down")
			}),
		}

		if _, _, err := client.FetchDailyDataStreaming(2, "2024-06-15", "2024-06-16"); err == nil {
			t.Fatal("expected transport error")
		}
	})

	t.Run("http status error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "bad gateway", http.StatusBadGateway)
		}))
		defer server.Close()

		client := NewOptimizedClient("test-key")
		client.SetBaseURL(server.URL)

		if _, _, err := client.FetchDailyDataStreaming(2, "2024-06-15", "2024-06-16"); err == nil {
			t.Fatal("expected status error")
		}
	})

	t.Run("decode error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"Data":{"Providers":"bad"}}`)
		}))
		defer server.Close()

		client := NewOptimizedClient("test-key")
		client.SetBaseURL(server.URL)

		if _, _, err := client.FetchDailyDataStreaming(2, "2024-06-15", "2024-06-16"); err == nil {
			t.Fatal("expected decode error")
		}
	})
}

func TestNewOptimizedClientBufferPool(t *testing.T) {
	client := NewOptimizedClient("test-key")
	buf, ok := client.bufferPool.Get().([]byte)
	if !ok {
		t.Fatalf("buffer pool returned %T, want []byte", buf)
	}
	if len(buf) != readBufferSize {
		t.Fatalf("buffer len = %d, want %d", len(buf), readBufferSize)
	}
}

func TestStreamingToDailyRecordInvalidDateAndQC(t *testing.T) {
	client := NewOptimizedClient("test-key")

	invalid := client.streamingToDailyRecord(StreamingDailyRecord{Date: "bad-date"}, 2)
	if invalid != (types.DailyRecord{}) {
		t.Fatalf("invalid date record = %+v, want zero", invalid)
	}

	record := client.streamingToDailyRecord(StreamingDailyRecord{
		Date:       "2024-06-15",
		DayAsceEto: &MinimalMeasurementValue{Value: 5.0, Qc: "Y"},
	}, 2)
	if record.QCFlags&0x02 == 0 {
		t.Fatalf("expected ET QC flag to be set, got %d", record.QCFlags)
	}
}

func TestStreamingToDailyRecordFallbackDateAndFields(t *testing.T) {
	client := NewOptimizedClient("test-key")

	record := client.streamingToDailyRecord(StreamingDailyRecord{
		Date:          "2101-01-02",
		DayAirTmpAvg:  &MinimalMeasurementValue{Value: 25.5, Qc: "R"},
		DayAsceEto:    &MinimalMeasurementValue{Value: 5.2, Qc: " "},
		DayWindSpdAvg: &MinimalMeasurementValue{Value: 3.1, Qc: " "},
		DayRelHumAvg:  &MinimalMeasurementValue{Value: 64, Qc: " "},
		DaySolRadAvg:  &MinimalMeasurementValue{Value: 2.2, Qc: " "},
	}, 4)

	if record.StationID != 4 {
		t.Fatalf("StationID = %d, want 4", record.StationID)
	}
	if record.QCFlags&0x01 == 0 {
		t.Fatalf("expected air temperature QC flag, got %d", record.QCFlags)
	}
	if record.Humidity != 64 || record.SolarRadiation == 0 {
		t.Fatalf("record fields not populated: %+v", record)
	}
}

func TestStreamDecodeDailyErrors(t *testing.T) {
	client := NewOptimizedClient("test-key")

	tests := []struct {
		name string
		body string
	}{
		{"malformed json", `{`},
		{"token error", `{"Data":`},
		{"missing providers", `{"Data":{"Other":[]}}`},
		{"invalid providers", `{"Data":{"Providers":"bad"}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := client.streamDecodeDaily(strings.NewReader(tt.body), 2); err == nil {
				t.Fatal("expected streamDecodeDaily error")
			}
		})
	}
}

func TestFetchMultipleStationsHTTP(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stationID := r.URL.Query().Get("stationNbrs")
		response := fmt.Sprintf(`{"Data":{"Providers":[{"Name":"CIMIS","Records":[{"Date":"2024-06-15","Station":%q,"DayAirTmpAvg":{"Value":"25.0","Qc":" "},"DayAsceEto":{"Value":"5.0","Qc":" "}}]}]}}`, stationID)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, response)
	}))
	defer server.Close()

	client := NewOptimizedClient("test-key")
	client.SetBaseURL(server.URL)

	results := client.FetchMultipleStations([]uint16{2, 3}, "2024-06-15", "2024-06-16", 1)
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
	for _, result := range results {
		if result.Error != nil {
			t.Fatalf("station %d error = %v", result.StationID, result.Error)
		}
		if len(result.Records) != 1 {
			t.Fatalf("station %d got %d records, want 1", result.StationID, len(result.Records))
		}
	}

	defaultPoolResults := client.FetchMultipleStations([]uint16{4}, "2024-06-15", "2024-06-16", 0)
	if len(defaultPoolResults) != 1 {
		t.Fatalf("default worker pool got %d results, want 1", len(defaultPoolResults))
	}
	if defaultPoolResults[0].Error != nil {
		t.Fatalf("default worker pool error = %v", defaultPoolResults[0].Error)
	}
}

func TestFetchMetricsString(t *testing.T) {
	metrics := &FetchMetrics{RecordsFetched: 3, BytesTransferred: 128}
	if got := metrics.String(); got == "" {
		t.Fatal("FetchMetrics.String() returned empty")
	}
}

func TestConvertHourlyToRecordsFast(t *testing.T) {
	apiRecords := []*HourlyDataRecord{
		{Date: "2024-06-15", Hour: "2300", HlyAirTmp: &MeasurementValue{Value: "28.5", Qc: "R"}},
		{Date: "bad-date", Hour: "0100", HlyAirTmp: &MeasurementValue{Value: "22.0", Qc: " "}},
	}

	records := ConvertHourlyToRecordsFast(apiRecords, 7)
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	if records[0].StationID != 7 {
		t.Errorf("StationID = %d, want 7", records[0].StationID)
	}
	if records[0].QCFlags&0x01 == 0 {
		t.Error("expected air temperature QC flag to be set")
	}
}

func TestConvertDailyToRecordsFastFallbackAndQC(t *testing.T) {
	apiRecords := []*DailyDataRecord{
		{Date: "2024-06-15", DayAirTmpAvg: &MeasurementValue{Value: "25.0", Qc: "R"}, DayAsceEto: &MeasurementValue{Value: "5.0", Qc: "Y"}},
		{Date: "2101-01-02", DayAirTmpAvg: &MeasurementValue{Value: "26.0", Qc: " "}},
		{Date: "bad-date", DayAirTmpAvg: &MeasurementValue{Value: "20.0", Qc: " "}},
	}

	records := ConvertDailyToRecordsFast(apiRecords, 3)
	if len(records) != 2 {
		t.Fatalf("got %d records, want 2", len(records))
	}
	if records[0].StationID != 3 {
		t.Fatalf("StationID = %d, want 3", records[0].StationID)
	}
	if records[0].QCFlags&0x01 == 0 || records[0].QCFlags&0x02 == 0 {
		t.Fatalf("expected temp and ET QC flags, got %d", records[0].QCFlags)
	}
	if records[1].Temperature == 0 {
		t.Fatalf("fallback date record was not populated: %+v", records[1])
	}
}

func TestMinimalToMeasurement(t *testing.T) {
	if got := minimalToMeasurement(nil); got != nil {
		t.Fatalf("minimalToMeasurement(nil) = %+v, want nil", got)
	}

	got := minimalToMeasurement(&MinimalMeasurementValue{Value: 12.345, Qc: "R"})
	if got == nil {
		t.Fatal("minimalToMeasurement() returned nil")
	}
	if got.Value != "12.35" || got.Qc != "R" {
		t.Errorf("minimalToMeasurement() = %+v", got)
	}
}

func TestSetHTTPClient(t *testing.T) {
	client := NewClient("test-key")
	custom := &http.Client{}
	client.SetHTTPClient(custom)
	if client.httpClient != custom {
		t.Fatal("SetHTTPClient did not replace client")
	}
}
