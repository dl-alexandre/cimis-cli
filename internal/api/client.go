// Package api provides a client for the CIMIS REST API.
// Updated to match actual CIMIS API response format.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/dl-alexandre/cimis-tsdb/types"
)

const (
	// BaseURL is the CIMIS Web API host.
	BaseURL = "https://et.water.ca.gov"

	// SubscriptionKeyHeader is the CIMIS API key header used by the current DWR API.
	SubscriptionKeyHeader = "Ocp-Apim-Subscription-Key"

	stationDataByNumberPath      = "/StationWeb/GetDataByStationNumber"
	stationDataByZipCodePath     = "/StationWeb/GetDataByStationZipCodes"
	spatialDataByCoordinatesPath = "/SpatialWeb/GetDataBySpatialCoordinates"
	spatialDataByAddressesPath   = "/SpatialWeb/GetDataByAddresses"
	spatialDataByZipCodePath     = "/SpatialWeb/GetDataBySpatialZipCodes"
	geoStationDataByZipCodePath  = "/GeoStationWeb/GetDataByGeoStationZipCodes"
	allStationsPath              = "/StationWeb/GetAllStations"
	stationByNumberPath          = "/StationWeb/GetStationByStationNumber"
	allStationZipCodesPath       = "/StationWeb/GetAllStationsZipCodes"
	stationZipCodeInfoPath       = "/StationWeb/GetStationZipCodeInfoByZipCode"
	allSpatialZipCodesPath       = "/SpatialWeb/GetAllSpatialZipCodes"
	spatialZipCodeInfoPath       = "/SpatialWeb/GetSpatialZipCodeInfoByZipCode"

	// DailyDataItems is the standard set of daily measurements requested from the API.
	DailyDataItems = "day-air-tmp-avg,day-asce-eto,day-wind-spd-avg,day-rel-hum-avg,day-sol-rad-avg,day-precip"

	// HourlyDataItems is the standard set of hourly measurements requested from the API.
	HourlyDataItems = "hly-air-tmp,hly-asce-eto,hly-wind-spd,hly-wind-dir,hly-rel-hum,hly-sol-rad,hly-precip,hly-vap-pres"

	// SpatialPointDataItems are the SCS measurements supported for coordinate and address requests.
	SpatialPointDataItems = "day-asce-eto,day-sol-rad-avg"

	// SpatialZipDataItems are the SCS measurements supported for spatial zip-code requests.
	SpatialZipDataItems = "day-asce-eto,day-sol-rad-avg,day-wind-spd-avg"

	// EpochYear is the base year for timestamp encoding (matches cimis-tsdb).
	EpochYear = 1985
)

// Epoch is the reference time for timestamp calculations.
var Epoch = time.Date(EpochYear, 1, 1, 0, 0, 0, 0, time.UTC)

var newHTTPRequest = http.NewRequestWithContext

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

func newCIMISRequest(ctx context.Context, baseURL, path string, params url.Values, appKey string) (*http.Request, string, error) {
	requestURL, err := buildCIMISURL(baseURL, path, params)
	if err != nil {
		return nil, "", err
	}

	req, err := newHTTPRequest(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Accept", "application/json")
	if appKey != "" {
		req.Header.Set(SubscriptionKeyHeader, appKey)
	}

	return req, requestURL, nil
}

func buildCIMISURL(baseURL, path string, params url.Values) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("parse base URL %q: %w", baseURL, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("base URL must include scheme and host: %q", baseURL)
	}

	basePath := strings.TrimRight(u.Path, "/")
	requestPath := strings.TrimLeft(path, "/")
	if requestPath != "" {
		u.Path = basePath + "/" + requestPath
	}
	u.RawQuery = params.Encode()

	return u.String(), nil
}

func (c *Client) getJSON(path string, params url.Values, target any) error {
	req, requestURL, err := newCIMISRequest(context.Background(), c.baseURL, path, params, c.appKey)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetch %s: %w", requestURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return apiError(resp.StatusCode, requestURL, body)
	}

	if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
		return fmt.Errorf("decode response from %s: %w", requestURL, err)
	}

	return nil
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
	Coordinate    string            `json:"Coordinate"`
	Address       string            `json:"Address"`
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

// Station describes a CIMIS weather station returned by the station endpoints.
type Station struct {
	StationNbr     string   `json:"StationNbr"`
	Name           string   `json:"Name"`
	City           string   `json:"City"`
	RegionalOffice string   `json:"RegionalOffice"`
	County         string   `json:"County"`
	ConnectDate    string   `json:"ConnectDate"`
	DisconnectDate string   `json:"DisconnectDate"`
	IsActive       string   `json:"IsActive"`
	IsEtoStation   string   `json:"IsEtoStation"`
	Elevation      string   `json:"Elevation"`
	GroundCover    string   `json:"GroundCover"`
	HmsLatitude    string   `json:"HmsLatitude"`
	HmsLongitude   string   `json:"HmsLongitude"`
	ZipCodes       []string `json:"ZipCodes"`
	SitingDesc     string   `json:"SitingDesc"`
}

// StationsResponse is the response shape for station metadata endpoints.
type StationsResponse struct {
	Stations []Station `json:"Stations"`
}

// ZipCodeInfo describes a CIMIS station or spatial zip code mapping.
type ZipCodeInfo struct {
	StationNbr     int    `json:"StationNbr,omitempty"`
	ZipCode        string `json:"ZipCode"`
	ConnectDate    string `json:"ConnectDate"`
	DisconnectDate string `json:"DisconnectDate"`
	IsActive       string `json:"IsActive"`
}

// ZipCodesResponse is the response shape for station and spatial zip code endpoints.
type ZipCodesResponse struct {
	ZipCodes []ZipCodeInfo `json:"ZipCodes"`
}

// Coordinate identifies a Spatial CIMIS location in decimal degrees.
type Coordinate struct {
	Lat float64
	Lng float64
}

// SpatialAddress identifies a geocoded Spatial CIMIS address target.
type SpatialAddress struct {
	Name    string
	Address string
}

// FetchDailyData retrieves daily data for a specific station and date range.
func (c *Client) FetchDailyData(stationID int, startDate, endDate string) ([]*DailyDataRecord, error) {
	params := url.Values{}
	params.Set("stationNbrs", strconv.Itoa(stationID))
	params.Set("startDate", NormalizeCIMISDate(startDate))
	params.Set("endDate", NormalizeCIMISDate(endDate))
	params.Set("isHourly", "false")
	params.Set("dataItems", DailyDataItems)
	params.Set("unitOfMeasure", "M")

	if requestURL, err := buildCIMISURL(c.baseURL, stationDataByNumberPath, params); err == nil {
		fmt.Printf("Fetching: %s\n", requestURL)
	}

	var apiResp APIResponse
	if err := c.getJSON(stationDataByNumberPath, params, &apiResp); err != nil {
		return nil, fmt.Errorf("fetch daily data for station %d (%s to %s): %w", stationID, startDate, endDate, err)
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
	params.Set("stationNbrs", strconv.Itoa(stationID))
	params.Set("startDate", NormalizeCIMISDate(startDate))
	params.Set("endDate", NormalizeCIMISDate(endDate))
	params.Set("isHourly", "true")
	params.Set("dataItems", HourlyDataItems)
	params.Set("unitOfMeasure", "M")

	if requestURL, err := buildCIMISURL(c.baseURL, stationDataByNumberPath, params); err == nil {
		fmt.Printf("Fetching hourly: %s\n", requestURL)
	}

	var apiResp HourlyAPIResponse
	if err := c.getJSON(stationDataByNumberPath, params, &apiResp); err != nil {
		return nil, fmt.Errorf("fetch hourly data for station %d (%s to %s): %w", stationID, startDate, endDate, err)
	}

	// Flatten records from all providers
	var records []*HourlyDataRecord
	for _, provider := range apiResp.Data.Providers {
		records = append(records, provider.Records...)
	}

	return records, nil
}

// FetchDailyDataByStationZipCodes retrieves daily WSN data for supported station zip codes.
func (c *Client) FetchDailyDataByStationZipCodes(zipCodes []string, startDate, endDate string) ([]*DailyDataRecord, error) {
	return c.fetchDailyByZipCodes(stationDataByZipCodePath, zipCodes, startDate, endDate, "")
}

// FetchHourlyDataByStationZipCodes retrieves hourly WSN data for supported station zip codes.
func (c *Client) FetchHourlyDataByStationZipCodes(zipCodes []string, startDate, endDate string) ([]*HourlyDataRecord, error) {
	params := url.Values{}
	params.Set("zipCodes", strings.Join(zipCodes, ","))
	params.Set("startDate", NormalizeCIMISDate(startDate))
	params.Set("endDate", NormalizeCIMISDate(endDate))
	params.Set("isHourly", "true")
	params.Set("dataItems", HourlyDataItems)
	params.Set("unitOfMeasure", "M")

	var apiResp HourlyAPIResponse
	if err := c.getJSON(stationDataByZipCodePath, params, &apiResp); err != nil {
		return nil, fmt.Errorf("fetch hourly data for zip codes %s (%s to %s): %w", strings.Join(zipCodes, ","), startDate, endDate, err)
	}

	var records []*HourlyDataRecord
	for _, provider := range apiResp.Data.Providers {
		records = append(records, provider.Records...)
	}
	return records, nil
}

// FetchDailyDataBySpatialZipCodes retrieves daily SCS data for supported spatial zip codes.
func (c *Client) FetchDailyDataBySpatialZipCodes(zipCodes []string, startDate, endDate string) ([]*DailyDataRecord, error) {
	return c.fetchDailyByZipCodes(spatialDataByZipCodePath, zipCodes, startDate, endDate, "")
}

// FetchDailyDataByGeoStationZipCodes retrieves daily data using DWR's WSN/SCS zip code selection logic.
func (c *Client) FetchDailyDataByGeoStationZipCodes(zipCodes []string, startDate, endDate, prefer string) ([]*DailyDataRecord, error) {
	return c.fetchDailyByZipCodes(geoStationDataByZipCodePath, zipCodes, startDate, endDate, prefer)
}

func (c *Client) fetchDailyByZipCodes(path string, zipCodes []string, startDate, endDate, prefer string) ([]*DailyDataRecord, error) {
	params := url.Values{}
	params.Set("zipCodes", strings.Join(zipCodes, ","))
	params.Set("startDate", NormalizeCIMISDate(startDate))
	params.Set("endDate", NormalizeCIMISDate(endDate))
	params.Set("isHourly", "false")
	params.Set("dataItems", dataItemsForZipCodePath(path))
	params.Set("unitOfMeasure", "M")
	if prefer != "" {
		params.Set("prefer", prefer)
	}

	var apiResp APIResponse
	if err := c.getJSON(path, params, &apiResp); err != nil {
		return nil, fmt.Errorf("fetch daily data for zip codes %s (%s to %s): %w", strings.Join(zipCodes, ","), startDate, endDate, err)
	}

	var records []*DailyDataRecord
	for _, provider := range apiResp.Data.Providers {
		records = append(records, provider.Records...)
	}

	return records, nil
}

func dataItemsForZipCodePath(path string) string {
	switch path {
	case spatialDataByZipCodePath, geoStationDataByZipCodePath:
		return SpatialZipDataItems
	default:
		return DailyDataItems
	}
}

// FetchDailyDataBySpatialCoordinates retrieves daily SCS data for decimal-degree coordinates.
func (c *Client) FetchDailyDataBySpatialCoordinates(coordinates []Coordinate, startDate, endDate string) ([]*DailyDataRecord, error) {
	params := url.Values{}
	params.Set("coordinates", formatCoordinates(coordinates))
	params.Set("startDate", NormalizeCIMISDate(startDate))
	params.Set("endDate", NormalizeCIMISDate(endDate))
	params.Set("dataItems", SpatialPointDataItems)
	params.Set("unitOfMeasure", "M")

	var apiResp APIResponse
	if err := c.getJSON(spatialDataByCoordinatesPath, params, &apiResp); err != nil {
		return nil, fmt.Errorf("fetch daily data for coordinates (%s to %s): %w", startDate, endDate, err)
	}

	var records []*DailyDataRecord
	for _, provider := range apiResp.Data.Providers {
		records = append(records, provider.Records...)
	}
	return records, nil
}

// FetchDailyDataBySpatialAddresses retrieves daily SCS data for street addresses.
func (c *Client) FetchDailyDataBySpatialAddresses(addresses []SpatialAddress, startDate, endDate string) ([]*DailyDataRecord, error) {
	params := url.Values{}
	params.Set("addresses", formatSpatialAddresses(addresses))
	params.Set("startDate", NormalizeCIMISDate(startDate))
	params.Set("endDate", NormalizeCIMISDate(endDate))
	params.Set("dataItems", SpatialPointDataItems)
	params.Set("unitOfMeasure", "M")

	var apiResp APIResponse
	if err := c.getJSON(spatialDataByAddressesPath, params, &apiResp); err != nil {
		return nil, fmt.Errorf("fetch daily data for addresses (%s to %s): %w", startDate, endDate, err)
	}

	var records []*DailyDataRecord
	for _, provider := range apiResp.Data.Providers {
		records = append(records, provider.Records...)
	}
	return records, nil
}

func formatCoordinates(coordinates []Coordinate) string {
	parts := make([]string, 0, len(coordinates))
	for _, coordinate := range coordinates {
		parts = append(parts, fmt.Sprintf("lat=%g,lng=%g", coordinate.Lat, coordinate.Lng))
	}
	return strings.Join(parts, ";")
}

func formatSpatialAddresses(addresses []SpatialAddress) string {
	parts := make([]string, 0, len(addresses))
	for _, address := range addresses {
		parts = append(parts, fmt.Sprintf("addr-name=%s,addr=%s", address.Name, address.Address))
	}
	return strings.Join(parts, ";")
}

// FetchAllStations retrieves CIMIS station metadata for all stations.
func (c *Client) FetchAllStations() ([]Station, error) {
	var resp StationsResponse
	if err := c.getJSON(allStationsPath, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Stations, nil
}

// FetchStation retrieves CIMIS station metadata by station number.
func (c *Client) FetchStation(stationID int) ([]Station, error) {
	params := url.Values{}
	params.Set("stationNbr", strconv.Itoa(stationID))

	var resp StationsResponse
	if err := c.getJSON(stationByNumberPath, params, &resp); err != nil {
		return nil, err
	}
	return resp.Stations, nil
}

// FetchAllStationZipCodes retrieves all zip codes supported by station data.
func (c *Client) FetchAllStationZipCodes() ([]ZipCodeInfo, error) {
	var resp ZipCodesResponse
	if err := c.getJSON(allStationZipCodesPath, nil, &resp); err != nil {
		return nil, err
	}
	return resp.ZipCodes, nil
}

// FetchStationZipCodeInfo retrieves station zip code support information by zip code.
func (c *Client) FetchStationZipCodeInfo(zipCode string) ([]ZipCodeInfo, error) {
	params := url.Values{}
	params.Set("zipCode", zipCode)

	var resp ZipCodesResponse
	if err := c.getJSON(stationZipCodeInfoPath, params, &resp); err != nil {
		return nil, err
	}
	return resp.ZipCodes, nil
}

// FetchAllSpatialZipCodes retrieves all zip codes supported by Spatial CIMIS.
func (c *Client) FetchAllSpatialZipCodes() ([]ZipCodeInfo, error) {
	var resp ZipCodesResponse
	if err := c.getJSON(allSpatialZipCodesPath, nil, &resp); err != nil {
		return nil, err
	}
	return resp.ZipCodes, nil
}

// FetchSpatialZipCodeInfo retrieves Spatial CIMIS zip code support information by zip code.
func (c *Client) FetchSpatialZipCodeInfo(zipCode string) ([]ZipCodeInfo, error) {
	params := url.Values{}
	params.Set("zipCode", zipCode)

	var resp ZipCodesResponse
	if err := c.getJSON(spatialZipCodeInfoPath, params, &resp); err != nil {
		return nil, err
	}
	return resp.ZipCodes, nil
}

// apiError builds a descriptive error for non-OK API responses with suggestions.
func apiError(statusCode int, requestURL string, body []byte) error {
	msg := fmt.Sprintf("API returned status %d for %s", statusCode, requestURL)
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

// ParseCIMISDate parses a CIMIS date string into time.Time.
func ParseCIMISDate(dateStr string) (time.Time, error) {
	if t, err := time.Parse("2006-01-02", dateStr); err == nil {
		return t, nil
	}
	return time.Parse("01/02/2006", dateStr)
}

// FormatCIMISDate formats a time.Time for the current CIMIS API (YYYY-MM-DD).
func FormatCIMISDate(t time.Time) string {
	return t.Format("2006-01-02")
}

// NormalizeCIMISDate converts supported CLI date inputs to the API's YYYY-MM-DD format.
func NormalizeCIMISDate(dateStr string) string {
	t, err := ParseCIMISDate(dateStr)
	if err != nil {
		return dateStr
	}
	return FormatCIMISDate(t)
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
