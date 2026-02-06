// Package api provides a client for the CIMIS REST API.
// Updated to match actual CIMIS API response format.
package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/dl-alexandre/cimis-tsdb/types"
)

const (
	// BaseURL is the CIMIS Web API endpoint.
	BaseURL = "http://et.water.ca.gov/api/data"

	// DailyDataItems is the standard set of daily measurements requested from the API.
	DailyDataItems = "day-air-tmp-avg,day-asce-eto,day-wind-spd-avg,day-rel-hum-avg,day-sol-rad-avg,day-precip"

	// HourlyDataItems is the standard set of hourly measurements requested from the API.
	HourlyDataItems = "hly-air-tmp,hly-asce-eto,hly-wind-spd,hly-wind-dir,hly-rel-hum,hly-sol-rad,hly-precip,hly-vap-pres"

	// EpochYear is the base year for timestamp encoding (matches cimis-tsdb).
	EpochYear = 1985
)

// Epoch is the reference time for timestamp calculations.
var Epoch = time.Date(EpochYear, 1, 1, 0, 0, 0, 0, time.UTC)

// Client handles requests to the CIMIS API.
type Client struct {
	appKey     string
	httpClient *http.Client
	baseURL    string
}

// NewClient creates a new CIMIS API client.
func NewClient(appKey string) *Client {
	return &Client{
		appKey: appKey,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		baseURL: BaseURL,
	}
}

// SetHTTPClient allows customizing the HTTP client (for testing).
func (c *Client) SetHTTPClient(client *http.Client) {
	c.httpClient = client
}

// SetBaseURL allows overriding the API base URL (for testing against mock servers).
func (c *Client) SetBaseURL(baseURL string) {
	c.baseURL = baseURL
}

// MeasurementValue represents a single measurement with value, QC code, and unit.
type MeasurementValue struct {
	Value string `json:"Value"`
	Qc    string `json:"Qc"`
	Unit  string `json:"Unit"`
}

// MinimalMeasurementValue is a low-allocation version for JSON decode.
// Uses float64 directly to avoid string allocations.
type MinimalMeasurementValue struct {
	Value float64 `json:"Value,string"`
	Qc    string  `json:"Qc"`
}

// DailyDataRecord represents the actual CIMIS API response structure for daily data.
type DailyDataRecord struct {
	Date          string            `json:"Date"`
	Julian        string            `json:"Julian"`
	Station       string            `json:"Station"`
	Standard      string            `json:"Standard"`
	ZipCodes      string            `json:"ZipCodes"`
	Scope         string            `json:"Scope"`
	DayAirTmpAvg  *MeasurementValue `json:"DayAirTmpAvg,omitempty"`
	DayAsceEto    *MeasurementValue `json:"DayAsceEto,omitempty"`
	DayWindSpdAvg *MeasurementValue `json:"DayWindSpdAvg,omitempty"`
	DayRelHumAvg  *MeasurementValue `json:"DayRelHumAvg,omitempty"`
	DaySolRadAvg  *MeasurementValue `json:"DaySolRadAvg,omitempty"`
	DayPrecip     *MeasurementValue `json:"DayPrecip,omitempty"`
}

// MinimalDailyRecord contains only fields we actually store for low-allocation decode.
type MinimalDailyRecord struct {
	Date          string                   `json:"Date"`
	DayAirTmpAvg  *MinimalMeasurementValue `json:"DayAirTmpAvg,omitempty"`
	DayAsceEto    *MinimalMeasurementValue `json:"DayAsceEto,omitempty"`
	DayWindSpdAvg *MinimalMeasurementValue `json:"DayWindSpdAvg,omitempty"`
	DayRelHumAvg  *MinimalMeasurementValue `json:"DayRelHumAvg,omitempty"`
	DaySolRadAvg  *MinimalMeasurementValue `json:"DaySolRadAvg,omitempty"`
	DayPrecip     *MinimalMeasurementValue `json:"DayPrecip,omitempty"`
}

// HourlyDataRecord represents the actual CIMIS API response structure for hourly data.
type HourlyDataRecord struct {
	Date       string            `json:"Date"`
	Hour       string            `json:"Hour"`
	Julian     string            `json:"Julian"`
	Station    string            `json:"Station"`
	Standard   string            `json:"Standard"`
	ZipCodes   string            `json:"ZipCodes"`
	Scope      string            `json:"Scope"`
	HlyAirTmp  *MeasurementValue `json:"HlyAirTmp,omitempty"`
	HlyAsceEto *MeasurementValue `json:"HlyAsceEto,omitempty"`
	HlyWindSpd *MeasurementValue `json:"HlyWindSpd,omitempty"`
	HlyWindDir *MeasurementValue `json:"HlyWindDir,omitempty"`
	HlyRelHum  *MeasurementValue `json:"HlyRelHum,omitempty"`
	HlySolRad  *MeasurementValue `json:"HlySolRad,omitempty"`
	HlyPrecip  *MeasurementValue `json:"HlyPrecip,omitempty"`
	HlyVapPres *MeasurementValue `json:"HlyVapPres,omitempty"`
}

// Provider represents a data provider in the CIMIS API response.
type Provider struct {
	Name    string             `json:"Name"`
	Type    string             `json:"Type"`
	Owner   string             `json:"Owner"`
	Records []*DailyDataRecord `json:"Records"`
}

// HourlyProvider represents a data provider for hourly data.
type HourlyProvider struct {
	Name    string              `json:"Name"`
	Type    string              `json:"Type"`
	Owner   string              `json:"Owner"`
	Records []*HourlyDataRecord `json:"Records"`
}

// APIResponse represents the top-level CIMIS API response.
type APIResponse struct {
	Data struct {
		Providers []Provider `json:"Providers"`
	} `json:"Data"`
}

// HourlyAPIResponse represents the top-level response for hourly data.
type HourlyAPIResponse struct {
	Data struct {
		Providers []HourlyProvider `json:"Providers"`
	} `json:"Data"`
}

// FetchDailyData retrieves daily data for a specific station and date range.
func (c *Client) FetchDailyData(stationID int, startDate, endDate string) ([]*DailyDataRecord, error) {
	params := url.Values{}
	params.Set("appKey", c.appKey)
	params.Set("targets", strconv.Itoa(stationID))
	params.Set("startDate", startDate)
	params.Set("endDate", endDate)
	params.Set("dataItems", DailyDataItems)
	params.Set("unitOfMeasure", "M")

	requestURL := fmt.Sprintf("%s?%s", c.baseURL, params.Encode())
	fmt.Printf("Fetching: %s\n", requestURL)

	resp, err := c.httpClient.Get(requestURL)
	if err != nil {
		return nil, fmt.Errorf("fetch daily data for station %d (%s to %s): %w", stationID, startDate, endDate, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, apiError(resp.StatusCode, stationID, startDate, endDate, body)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response for station %d: %w", stationID, err)
	}

	var apiResp APIResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, fmt.Errorf("decode response for station %d: %w", stationID, err)
	}

	// Flatten records from all providers
	var records []*DailyDataRecord
	for _, provider := range apiResp.Data.Providers {
		records = append(records, provider.Records...)
	}

	return records, nil
}

// FetchHourlyData retrieves hourly data for a specific station and date range.
func (c *Client) FetchHourlyData(stationID int, startDate, endDate string) ([]*HourlyDataRecord, error) {
	params := url.Values{}
	params.Set("appKey", c.appKey)
	params.Set("targets", strconv.Itoa(stationID))
	params.Set("startDate", startDate)
	params.Set("endDate", endDate)
	params.Set("dataItems", HourlyDataItems)
	params.Set("unitOfMeasure", "M")

	requestURL := fmt.Sprintf("%s?%s", c.baseURL, params.Encode())
	fmt.Printf("Fetching hourly: %s\n", requestURL)

	resp, err := c.httpClient.Get(requestURL)
	if err != nil {
		return nil, fmt.Errorf("fetch hourly data for station %d (%s to %s): %w", stationID, startDate, endDate, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, apiError(resp.StatusCode, stationID, startDate, endDate, body)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response for station %d: %w", stationID, err)
	}

	var apiResp HourlyAPIResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, fmt.Errorf("decode response for station %d: %w", stationID, err)
	}

	// Flatten records from all providers
	var records []*HourlyDataRecord
	for _, provider := range apiResp.Data.Providers {
		records = append(records, provider.Records...)
	}

	return records, nil
}

// apiError builds a descriptive error for non-OK API responses with suggestions.
func apiError(statusCode, stationID int, startDate, endDate string, body []byte) error {
	msg := fmt.Sprintf("API returned status %d for station %d (%s to %s)", statusCode, stationID, startDate, endDate)
	if len(body) > 0 {
		msg += ": " + string(body)
	}
	switch {
	case statusCode == 401 || statusCode == 403:
		msg += "\n  Hint: check that CIMIS_APP_KEY is set and valid"
	case statusCode == 429:
		msg += "\n  Hint: rate limited — reduce -concurrency or wait before retrying"
	case statusCode >= 500:
		msg += "\n  Hint: CIMIS server error — retry later"
	}
	return fmt.Errorf("%s", msg)
}

// ParseCIMISDate parses a CIMIS date string (MM/DD/YYYY) into time.Time.
func ParseCIMISDate(dateStr string) (time.Time, error) {
	return time.Parse("01/02/2006", dateStr)
}

// FormatCIMISDate formats a time.Time for CIMIS API (MM/DD/YYYY).
func FormatCIMISDate(t time.Time) string {
	return t.Format("01/02/2006")
}

// ParseMeasurementValue safely parses a string value to float64.
func ParseMeasurementValue(mv *MeasurementValue) float64 {
	if mv == nil {
		return 0
	}
	val, err := strconv.ParseFloat(mv.Value, 64)
	if err != nil {
		return 0
	}
	return val
}

// HasQCFlag checks if a measurement has a QC flag indicating an issue.
func HasQCFlag(mv *MeasurementValue) bool {
	if mv == nil {
		return false
	}
	// QC flag " " means good data, anything else indicates an issue
	return mv.Qc != " " && mv.Qc != ""
}

// ConvertDailyToRecords converts CIMIS API daily records to our binary format.
func ConvertDailyToRecords(apiRecords []*DailyDataRecord, stationID uint16) []types.DailyRecord {
	records := make([]types.DailyRecord, 0, len(apiRecords))

	for _, apiRec := range apiRecords {
		date, err := time.Parse("2006-01-02", apiRec.Date)
		if err != nil {
			continue
		}

		record := types.DailyRecord{
			Timestamp:      types.TimeToDaysSinceEpoch(date),
			StationID:      stationID,
			Temperature:    types.ScaleTemperature(ParseMeasurementValue(apiRec.DayAirTmpAvg)),
			ET:             types.ScaleET(ParseMeasurementValue(apiRec.DayAsceEto)),
			WindSpeed:      types.ScaleWindSpeed(ParseMeasurementValue(apiRec.DayWindSpdAvg)),
			Humidity:       uint8(ParseMeasurementValue(apiRec.DayRelHumAvg)),
			SolarRadiation: uint8(ParseMeasurementValue(apiRec.DaySolRadAvg) * 10), // Scale to tenths
		}

		// Set QC flags if any measurements have issues
		if HasQCFlag(apiRec.DayAirTmpAvg) {
			record.QCFlags |= 0x01
		}
		if HasQCFlag(apiRec.DayAsceEto) {
			record.QCFlags |= 0x02
		}

		records = append(records, record)
	}

	return records
}

// ConvertHourlyToRecords converts CIMIS API hourly records to our binary format.
func ConvertHourlyToRecords(apiRecords []*HourlyDataRecord, stationID uint16) []types.HourlyRecord {
	records := make([]types.HourlyRecord, 0, len(apiRecords))

	for _, apiRec := range apiRecords {
		date, err := time.Parse("2006-01-02", apiRec.Date)
		if err != nil {
			continue
		}

		// Parse hour (format is "HH:00")
		hour := 0
		if len(apiRec.Hour) >= 2 {
			hour, _ = strconv.Atoi(apiRec.Hour[:2])
		}

		timestamp := date.Add(time.Duration(hour) * time.Hour)

		record := types.HourlyRecord{
			Timestamp:      types.TimeToHoursSinceEpoch(timestamp),
			StationID:      stationID,
			Temperature:    types.ScaleTemperature(ParseMeasurementValue(apiRec.HlyAirTmp)),
			ET:             types.ScaleHourlyET(ParseMeasurementValue(apiRec.HlyAsceEto)),
			WindSpeed:      types.ScaleWindSpeed(ParseMeasurementValue(apiRec.HlyWindSpd)),
			WindDirection:  uint8(ParseMeasurementValue(apiRec.HlyWindDir) / 2),
			Humidity:       uint8(ParseMeasurementValue(apiRec.HlyRelHum)),
			SolarRadiation: uint16(ParseMeasurementValue(apiRec.HlySolRad)),
			Precipitation:  types.ScalePrecip(ParseMeasurementValue(apiRec.HlyPrecip)),
			VaporPressure:  types.ScaleVapor(ParseMeasurementValue(apiRec.HlyVapPres)),
		}

		// Set QC flags
		if HasQCFlag(apiRec.HlyAirTmp) {
			record.QCFlags |= 0x01
		}

		records = append(records, record)
	}

	return records
}

// parseDateYYYYMMDD parses "YYYY-MM-DD" format without time.Parse overhead.
// Returns year, month, day and ok flag. No allocations.
func parseDateYYYYMMDD(s string) (year, month, day int, ok bool) {
	if len(s) != 10 {
		return 0, 0, 0, false
	}
	// Expected: "2006-01-02"
	if s[4] != '-' || s[7] != '-' {
		return 0, 0, 0, false
	}
	year = (int(s[0]-'0')*1000 + int(s[1]-'0')*100 + int(s[2]-'0')*10 + int(s[3]-'0'))
	month = (int(s[5]-'0')*10 + int(s[6]-'0'))
	day = (int(s[8]-'0')*10 + int(s[9]-'0'))
	// Basic validation
	if year < 1985 || year > 2100 || month < 1 || month > 12 || day < 1 || day > 31 {
		return 0, 0, 0, false
	}
	return year, month, day, true
}

// daysSinceEpoch computes days since 1985-01-01 from YYYY-MM-DD components.
// Uses the same calculation as types.TimeToDaysSinceEpoch for consistency.
func daysSinceEpoch(year, month, day int) uint32 {
	t := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	return types.TimeToDaysSinceEpoch(t)
}

// ConvertDailyToRecordsFast converts daily records with manual date parsing.
// This is ~10x faster than time.Parse for the "YYYY-MM-DD" format.
func ConvertDailyToRecordsFast(apiRecords []*DailyDataRecord, stationID uint16) []types.DailyRecord {
	records := make([]types.DailyRecord, 0, len(apiRecords))

	for _, apiRec := range apiRecords {
		// Try manual parse first for performance (no allocations)
		year, month, day, ok := parseDateYYYYMMDD(apiRec.Date)
		var ts uint32
		if ok {
			ts = daysSinceEpoch(year, month, day)
		} else {
			// Fallback to time.Parse for edge cases
			date, err := time.Parse("2006-01-02", apiRec.Date)
			if err != nil {
				continue
			}
			ts = types.TimeToDaysSinceEpoch(date)
		}

		record := types.DailyRecord{
			Timestamp:      ts,
			StationID:      stationID,
			Temperature:    types.ScaleTemperature(ParseMeasurementValue(apiRec.DayAirTmpAvg)),
			ET:             types.ScaleET(ParseMeasurementValue(apiRec.DayAsceEto)),
			WindSpeed:      types.ScaleWindSpeed(ParseMeasurementValue(apiRec.DayWindSpdAvg)),
			Humidity:       uint8(ParseMeasurementValue(apiRec.DayRelHumAvg)),
			SolarRadiation: uint8(ParseMeasurementValue(apiRec.DaySolRadAvg) * 10),
		}

		// Set QC flags
		if HasQCFlag(apiRec.DayAirTmpAvg) {
			record.QCFlags |= 0x01
		}
		if HasQCFlag(apiRec.DayAsceEto) {
			record.QCFlags |= 0x02
		}

		records = append(records, record)
	}

	return records
}

// ConvertHourlyToRecordsFast converts hourly records with manual date parsing.
func ConvertHourlyToRecordsFast(apiRecords []*HourlyDataRecord, stationID uint16) []types.HourlyRecord {
	records := make([]types.HourlyRecord, 0, len(apiRecords))

	for _, apiRec := range apiRecords {
		// Parse date with fast path
		year, month, day, ok := parseDateYYYYMMDD(apiRec.Date)
		var date time.Time
		if ok {
			date = time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
		} else {
			var err error
			date, err = time.Parse("2006-01-02", apiRec.Date)
			if err != nil {
				continue
			}
		}

		// Parse hour (format is "HH:00")
		hour := 0
		if len(apiRec.Hour) >= 2 {
			hour, _ = strconv.Atoi(apiRec.Hour[:2])
		}

		timestamp := date.Add(time.Duration(hour) * time.Hour)

		record := types.HourlyRecord{
			Timestamp:      types.TimeToHoursSinceEpoch(timestamp),
			StationID:      stationID,
			Temperature:    types.ScaleTemperature(ParseMeasurementValue(apiRec.HlyAirTmp)),
			ET:             types.ScaleHourlyET(ParseMeasurementValue(apiRec.HlyAsceEto)),
			WindSpeed:      types.ScaleWindSpeed(ParseMeasurementValue(apiRec.HlyWindSpd)),
			WindDirection:  uint8(ParseMeasurementValue(apiRec.HlyWindDir) / 2),
			Humidity:       uint8(ParseMeasurementValue(apiRec.HlyRelHum)),
			SolarRadiation: uint16(ParseMeasurementValue(apiRec.HlySolRad)),
			Precipitation:  types.ScalePrecip(ParseMeasurementValue(apiRec.HlyPrecip)),
			VaporPressure:  types.ScaleVapor(ParseMeasurementValue(apiRec.HlyVapPres)),
		}

		// Set QC flags
		if HasQCFlag(apiRec.HlyAirTmp) {
			record.QCFlags |= 0x01
		}

		records = append(records, record)
	}

	return records
}

// minimalToMeasurement converts a minimal measurement to standard format.
func minimalToMeasurement(min *MinimalMeasurementValue) *MeasurementValue {
	if min == nil {
		return nil
	}
	return &MeasurementValue{
		Value: fmt.Sprintf("%.2f", min.Value),
		Qc:    min.Qc,
		Unit:  "", // Unit not stored in minimal format
	}
}

// ConvertMinimalDailyToRecords converts minimal daily records directly to binary format.
// This avoids intermediate MeasurementValue allocations entirely.
func ConvertMinimalDailyToRecords(minRecords []MinimalDailyRecord, stationID uint16) []types.DailyRecord {
	records := make([]types.DailyRecord, 0, len(minRecords))

	for _, apiRec := range minRecords {
		// Fast date parse
		year, month, day, ok := parseDateYYYYMMDD(apiRec.Date)
		var ts uint32
		if ok {
			ts = daysSinceEpoch(year, month, day)
		} else {
			date, err := time.Parse("2006-01-02", apiRec.Date)
			if err != nil {
				continue
			}
			ts = types.TimeToDaysSinceEpoch(date)
		}

		// Extract values directly without intermediate structs
		var temp, et, wind, humidity, solar float64
		var qcFlags uint8

		if apiRec.DayAirTmpAvg != nil {
			temp = apiRec.DayAirTmpAvg.Value
			if apiRec.DayAirTmpAvg.Qc != " " && apiRec.DayAirTmpAvg.Qc != "" {
				qcFlags |= 0x01
			}
		}
		if apiRec.DayAsceEto != nil {
			et = apiRec.DayAsceEto.Value
			if apiRec.DayAsceEto.Qc != " " && apiRec.DayAsceEto.Qc != "" {
				qcFlags |= 0x02
			}
		}
		if apiRec.DayWindSpdAvg != nil {
			wind = apiRec.DayWindSpdAvg.Value
		}
		if apiRec.DayRelHumAvg != nil {
			humidity = apiRec.DayRelHumAvg.Value
		}
		if apiRec.DaySolRadAvg != nil {
			solar = apiRec.DaySolRadAvg.Value
		}

		record := types.DailyRecord{
			Timestamp:      ts,
			StationID:      stationID,
			Temperature:    types.ScaleTemperature(temp),
			ET:             types.ScaleET(et),
			WindSpeed:      types.ScaleWindSpeed(wind),
			Humidity:       uint8(humidity),
			SolarRadiation: uint8(solar * 10),
			QCFlags:        qcFlags,
		}

		records = append(records, record)
	}

	return records
}
