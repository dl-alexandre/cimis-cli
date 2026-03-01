# CIMIS CLI - Code Patterns & Best Practices

## 1. External URL Handling & Browser Opening

### Pattern: Cross-Platform Browser Opening
**Location:** `cmd/cimis/browser.go`

The codebase implements a clean, cross-platform pattern for opening URLs in the default browser:

```go
func openBrowser(url string) error {
	var cmd string
	var args []string

	switch runtime.GOOS {
	case "windows":
		cmd = "cmd"
		args = []string{"/c", "start", url}
	case "darwin":
		cmd = "open"
		args = []string{url}
	default:
		cmd = "xdg-open"
		args = []string{url}
	}

	return exec.Command(cmd, args...).Start()
}
```

**Key Characteristics:**
- Uses `runtime.GOOS` for platform detection (Windows, macOS, Linux)
- Uses `exec.Command(...).Start()` (non-blocking) instead of `.Run()` (blocking)
- Returns error for caller to handle
- Simple, no external dependencies

### Usage Pattern
Commands that open URLs follow this pattern:

```go
func cmdRegister() {
	const registerURL = "https://cimis.water.ca.gov/Welcome.aspx"

	fmt.Println("Opening CIMIS registration page in your browser...")
	fmt.Printf("URL: %s\n", registerURL)

	if err := openBrowser(registerURL); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open browser: %v\n", err)
		fmt.Fprintf(os.Stderr, "Please manually visit: %s\n", registerURL)
		os.Exit(1)
	}

	fmt.Println("Browser opened successfully!")
	// ... additional instructions
}
```

**Error Handling:**
- Prints error to stderr
- Provides fallback instructions (manual URL)
- Exits with status 1 on failure
- Provides helpful next steps to user

### Existing Browser Commands
- `register` - Opens CIMIS registration page
- `login` - Opens CIMIS login page  
- `api-docs` - Opens CIMIS API documentation

---

## 2. HTTP Client & External API Patterns

### Pattern: HTTP Client Configuration
**Location:** `internal/api/client.go`, `internal/api/client_streaming.go`

#### Basic HTTP Client Setup
```go
type Client struct {
	appKey     string
	httpClient *http.Client
	baseURL    string
}

func NewClient(appKey string) *Client {
	return &Client{
		appKey: appKey,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		baseURL: BaseURL,
	}
}

// Allow customization for testing
func (c *Client) SetHTTPClient(client *http.Client) {
	c.httpClient = client
}

func (c *Client) SetBaseURL(baseURL string) {
	c.baseURL = baseURL
}
```

**Key Characteristics:**
- Encapsulates HTTP client in struct
- 30-second timeout (prevents hanging)
- Allows injection for testing
- Supports base URL override

#### Optimized HTTP Transport
**Location:** `internal/api/client_streaming.go`

```go
func OptimizedHTTPTransport() *http.Transport {
	return &http.Transport{
		// Connection pooling
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		MaxConnsPerHost:     10,
		
		// Timeouts
		DialTimeout:         10 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		
		// Keep-alive
		DisableKeepAlives: false,
		IdleConnTimeout:   90 * time.Second,
	}
}
```

**Use Case:** For streaming/batch operations with multiple concurrent requests

### API Request Pattern
```go
func (c *Client) FetchHourlyData(stationID int, startDate, endDate string) ([]*HourlyDataRecord, error) {
	params := url.Values{}
	params.Set("appKey", c.appKey)
	params.Set("stationIds", strconv.Itoa(stationID))
	params.Set("startDate", startDate)
	params.Set("endDate", endDate)
	params.Set("dataItems", HourlyDataItems)

	requestURL := c.baseURL + "?" + params.Encode()
	fmt.Printf("Fetching hourly: %s\n", requestURL)

	resp, err := c.httpClient.Get(requestURL)
	if err != nil {
		return nil, fmt.Errorf("fetch hourly data for station %d (%s to %s): %w", stationID, startDate, endDate, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API returned status %d: %s", resp.StatusCode, string(body))
	}

	var apiResp HourlyAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return apiResp.Providers[0].Records, nil
}
```

**Key Characteristics:**
- Uses `url.Values` for query parameters
- Wraps errors with context using `fmt.Errorf` and `%w`
- Checks HTTP status code explicitly
- Defers response body close
- Uses `json.NewDecoder` for streaming JSON

---

## 3. Error Handling Patterns

### Pattern 1: Fatal Errors in CLI Commands
**Location:** All `cmd/cimis/*.go` files

Used for unrecoverable errors that should terminate the program:

```go
// Missing required configuration
if appKey == "" {
	log.Fatal("CIMIS app key required (use -app-key flag or CIMIS_APP_KEY env var)")
}

// Flag parsing errors
if err := fs.Parse(args); err != nil {
	log.Fatal(err)
}

// Missing required arguments
if *stationID == 0 {
	log.Fatal("Station ID required")
}

// File system errors
if err := os.MkdirAll(dir, 0755); err != nil {
	log.Fatalf("Failed to create directory %s: %v", dir, err)
}
```

**Pattern:**
- Use `log.Fatal()` for simple messages
- Use `log.Fatalf()` for formatted messages with context
- Always include what failed and why
- Automatically exits with status 1

### Pattern 2: Error Wrapping with Context
**Location:** `internal/api/client.go`

```go
// Wrap errors with context about what operation failed
return nil, fmt.Errorf("fetch hourly data for station %d (%s to %s): %w", 
	stationID, startDate, endDate, err)

// For parsing errors
return nil, fmt.Errorf("invalid range format: %s", part)

// For validation errors
return nil, fmt.Errorf("invalid station ID: %s", part)
```

**Pattern:**
- Use `fmt.Errorf` with `%w` to wrap errors
- Include context about what was being attempted
- Include relevant parameters/values
- Allows error chain inspection with `errors.Is()` and `errors.As()`

### Pattern 3: Graceful Degradation (Browser Opening)
**Location:** `cmd/cimis/browser.go`

```go
if err := openBrowser(registerURL); err != nil {
	fmt.Fprintf(os.Stderr, "Failed to open browser: %v\n", err)
	fmt.Fprintf(os.Stderr, "Please manually visit: %s\n", registerURL)
	os.Exit(1)  // Still exit, but with helpful fallback
}
```

**Pattern:**
- Attempt operation
- On error, provide fallback instructions
- Print to stderr for errors
- Exit with non-zero status

### Pattern 4: Validation Errors
**Location:** `cmd/cimis/fetch.go`, `cmd/cimis/query.go`

```go
// Date parsing
startDate, err := time.Parse("2006-01-02", *start)
if err != nil {
	log.Fatalf("Invalid start date: %v", err)
}

// Cache size parsing
cacheSize := parseCacheSize(*cache)
if cacheSize == 0 && *cache != "" {
	log.Fatalf("Invalid cache size: %s", *cache)
}
```

**Pattern:**
- Validate inputs early
- Use `log.Fatalf` with clear error messages
- Include the invalid value in the error message

---

## 4. External System Calls Pattern

### Pattern: Subprocess Execution
**Location:** `cmd/cimis/integration_test.go`

```go
// Build subprocess
buildCmd := exec.Command("go", "build", "-o", cliPath, ".")
output, err := buildCmd.CombinedOutput()
if err != nil {
	t.Fatalf("Build failed: %v\nOutput: %s", err, string(output))
}

// Run CLI command
cmd := exec.Command(cliPath, "init")
cmd.Env = append(os.Environ(), "CIMIS_APP_KEY="+appKey)
cmd.Dir = tmpDir
output, err := cmd.CombinedOutput()
if err != nil {
	t.Fatalf("Command failed: %v\nOutput: %s", err, string(output))
}
```

**Key Characteristics:**
- Use `exec.Command()` for subprocess execution
- Use `CombinedOutput()` to capture both stdout and stderr
- Set environment variables via `cmd.Env`
- Set working directory via `cmd.Dir`
- Include output in error messages for debugging

### Pattern: Non-Blocking vs Blocking Execution
```go
// Non-blocking (for browser opening)
return exec.Command(cmd, args...).Start()

// Blocking (for build/test operations)
output, err := cmd.CombinedOutput()
```

---

## 5. Command Structure Pattern

### Main Entry Point
**Location:** `cmd/cimis/main.go`

```go
func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	// Global flags
	dataDir := flag.String("data-dir", "./data", "Data directory path")
	appKey := flag.String("app-key", os.Getenv("CIMIS_APP_KEY"), "CIMIS API app key")

	// Subcommand dispatch
	switch os.Args[1] {
	case "version":
		fmt.Printf("cimis version %s (built %s)\n", Version, BuildTime)
	case "init":
		cmdInit(*dataDir)
	case "register":
		cmdRegister()
	// ... more commands
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}
```

**Pattern:**
- Check for minimum arguments
- Define global flags before switch
- Use switch for subcommand dispatch
- Call separate `cmd*` functions for each command
- Print usage on error
- Exit with status 1 on error

### Individual Command Pattern
```go
func cmdFetch(dataDir, appKey string, args []string) {
	// 1. Validate required inputs
	if appKey == "" {
		log.Fatal("CIMIS app key required...")
	}

	// 2. Parse command-specific flags
	fs := flag.NewFlagSet("fetch", flag.ExitOnError)
	stationID := fs.Int("station", 0, "Station ID")
	days := fs.Int("days", 7, "Number of days to fetch")
	
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}

	// 3. Validate parsed arguments
	if *stationID == 0 {
		log.Fatal("Station ID required")
	}

	// 4. Perform operation
	client := api.NewClient(appKey)
	records, err := client.FetchHourlyData(...)
	if err != nil {
		log.Fatalf("Failed to fetch hourly data: %v", err)
	}

	// 5. Report results
	fmt.Printf("Fetched %d hourly records for station %d\n", len(records), *stationID)
}
```

**Pattern:**
1. Validate required inputs (env vars, config)
2. Parse command-specific flags
3. Validate parsed arguments
4. Perform operation
5. Report results

---

## 6. Testing Patterns

### HTTP Mock Testing
**Location:** `internal/api/client_test.go`

```go
server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}))
defer server.Close()

client := NewClient("test-key")
client.SetBaseURL(server.URL)
client.SetHTTPClient(server.Client())

// Test the client
records, err := client.FetchHourlyData(2, "06/15/2024", "06/15/2024")
```

**Pattern:**
- Use `httptest.NewServer()` for mock HTTP server
- Use `SetBaseURL()` and `SetHTTPClient()` for injection
- Defer server close
- Test against mock responses

---

## 7. Recommended Patterns for New Features

### For Opening External URLs/Browsers:
1. Use the existing `openBrowser()` function in `browser.go`
2. Follow the error handling pattern: try to open, provide fallback URL
3. Print helpful instructions to user
4. Exit with status 1 on failure

### For External API Calls:
1. Create a client struct with `http.Client` field
2. Set reasonable timeouts (30s for general, 10s for streaming)
3. Use `url.Values` for query parameters
4. Wrap errors with context using `fmt.Errorf` and `%w`
5. Check HTTP status codes explicitly
6. Defer response body close
7. Use `json.NewDecoder` for streaming JSON

### For Error Handling:
1. Use `log.Fatal()` / `log.Fatalf()` for unrecoverable errors in CLI
2. Use `fmt.Errorf` with `%w` for library functions
3. Always include context about what failed
4. Include relevant values in error messages
5. Print errors to stderr
6. Exit with status 1 on fatal errors

### For System Calls:
1. Use `exec.Command()` for subprocess execution
2. Use `.Start()` for non-blocking (browser opening)
3. Use `.CombinedOutput()` for blocking with output capture
4. Set environment variables and working directory as needed
5. Include subprocess output in error messages

### For CLI Commands:
1. Validate required inputs early
2. Parse flags with `flag.NewFlagSet`
3. Validate parsed arguments
4. Perform operation
5. Report results clearly
6. Use consistent error messages

---

## Summary

The CIMIS CLI follows clean, idiomatic Go patterns:
- **Cross-platform compatibility** via `runtime.GOOS`
- **Dependency injection** for testability
- **Error wrapping** with context
- **Graceful degradation** where possible
- **Clear separation** between CLI and library code
- **Consistent error handling** across commands
