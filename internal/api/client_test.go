package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestParseCIMISDate(t *testing.T) {
	tests := []struct {
		input   string
		wantErr bool
		want    time.Time
	}{
		{"01/15/2024", false, time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)},
		{"12/31/2023", false, time.Date(2023, 12, 31, 0, 0, 0, 0, time.UTC)},
		{"02/29/2024", false, time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)}, // leap year
		{"", true, time.Time{}},
		{"2024-01-15", true, time.Time{}}, // wrong format
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
		{time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), "01/15/2024"},
		{time.Date(2023, 12, 31, 0, 0, 0, 0, time.UTC), "12/31/2023"},
		{time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC), "02/29/2024"},
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
		input              string
		wantYear, wantMonth, wantDay int
		wantOK             bool
	}{
		{"2024-01-15", 2024, 1, 15, true},
		{"1985-01-01", 1985, 1, 1, true},
		{"2099-12-31", 2099, 12, 31, true},
		{"", 0, 0, 0, false},
		{"short", 0, 0, 0, false},
		{"2024/01/15", 0, 0, 0, false},   // wrong separator
		{"20240115xx", 0, 0, 0, false},    // wrong length ok but wrong format
		{"1984-01-01", 0, 0, 0, false},   // before epoch
		{"2101-01-01", 0, 0, 0, false},   // after 2100
		{"2024-13-01", 0, 0, 0, false},   // invalid month
		{"2024-00-01", 0, 0, 0, false},   // month 0
		{"2024-01-00", 0, 0, 0, false},   // day 0
		{"2024-01-32", 0, 0, 0, false},   // day 32
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
			Date:         "2024-06-15",
			DayAirTmpAvg: &MeasurementValue{Value: "25.3", Qc: " "},
			DayAsceEto:   &MeasurementValue{Value: "5.2", Qc: " "},
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
		// Verify expected query params
		q := r.URL.Query()
		if q.Get("appKey") != "test-key" {
			t.Errorf("appKey = %q, want test-key", q.Get("appKey"))
		}
		if q.Get("targets") != "2" {
			t.Errorf("targets = %q, want 2", q.Get("targets"))
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

func TestConvertMinimalDailyToRecords(t *testing.T) {
	minRecords := []MinimalDailyRecord{
		{
			Date:         "2024-06-15",
			DayAirTmpAvg: &MinimalMeasurementValue{Value: 25.3, Qc: " "},
			DayAsceEto:   &MinimalMeasurementValue{Value: 5.2, Qc: " "},
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
