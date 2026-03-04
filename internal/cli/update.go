package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// UpdateInfo holds the update check result
type UpdateInfo struct {
	CurrentVersion  string `json:"current_version"`
	LatestVersion   string `json:"latest_version"`
	UpdateAvailable bool   `json:"update_available"`
	ReleaseURL      string `json:"release_url"`
	PublishedAt     string `json:"published_at"`
	IsPrerelease    bool   `json:"is_prerelease"`
}

// GitHubRelease represents the release information from GitHub API
type GitHubRelease struct {
	TagName     string    `json:"tag_name"`
	Name        string    `json:"name"`
	PublishedAt time.Time `json:"published_at"`
	HTMLURL     string    `json:"html_url"`
	Body        string    `json:"body"`
	Prerelease  bool      `json:"prerelease"`
	Draft       bool      `json:"draft"`
}

const (
	cacheKey = "update_check"
	cacheTTL = 24 * time.Hour // Check once per day
)

// CheckForUpdates performs a manual update check and displays results
func CheckForUpdates(force bool, format string) error {
	currentVersion := Version
	if currentVersion == "" || currentVersion == "dev" {
		currentVersion = "v0.0.0"
	}

	// Try to get from cache first
	if !force {
		if cached, ok := getCachedUpdateInfo(); ok {
			return displayUpdateInfo(cached, format)
		}
	}

	// Fetch latest release from GitHub
	info, err := fetchLatestRelease(currentVersion)
	if err != nil {
		return fmt.Errorf("failed to check for updates: %w", err)
	}

	// Cache the result
	cacheUpdateInfo(info)

	return displayUpdateInfo(info, format)
}

// fetchLatestRelease queries GitHub API for the latest release
func fetchLatestRelease(currentVersion string) (UpdateInfo, error) {
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	apiURL := buildGitHubAPIURL()

	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return UpdateInfo{}, err
	}

	// GitHub API requires a User-Agent header
	req.Header.Set("User-Agent", fmt.Sprintf("%s/%s", BinaryName, currentVersion))
	req.Header.Set("Accept", "application/vnd.github.v3+json")

	resp, err := client.Do(req)
	if err != nil {
		return UpdateInfo{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return UpdateInfo{}, fmt.Errorf("GitHub API returned %d: %s", resp.StatusCode, string(body))
	}

	var release GitHubRelease
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return UpdateInfo{}, err
	}

	// Normalize version strings
	latestVersion := normalizeVersion(release.TagName)
	currentNormalized := normalizeVersion(currentVersion)

	// Compare versions
	updateAvailable := compareVersions(currentNormalized, latestVersion) < 0

	return UpdateInfo{
		CurrentVersion:  currentVersion,
		LatestVersion:   latestVersion,
		UpdateAvailable: updateAvailable,
		ReleaseURL:      release.HTMLURL,
		PublishedAt:     release.PublishedAt.Format("2006-01-02"),
		IsPrerelease:    release.Prerelease,
	}, nil
}

// displayUpdateInfo shows the update information to the user
func displayUpdateInfo(info UpdateInfo, format string) error {
	switch format {
	case "json":
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(info)
	default:
		if info.UpdateAvailable {
			fmt.Println()
			fmt.Println("╔══════════════════════════════════════════════════════════════╗")
			fmt.Println("║                    UPDATE AVAILABLE                            ║")
			fmt.Println("╚══════════════════════════════════════════════════════════════╝")
			fmt.Println()
			fmt.Printf("Current version: %s\n", info.CurrentVersion)
			fmt.Printf("Latest version:  %s\n", info.LatestVersion)
			fmt.Printf("Published:       %s\n", info.PublishedAt)
			fmt.Println()
			fmt.Println("Install the latest version:")
			fmt.Printf("  brew upgrade %s\n", BinaryName)
			fmt.Println()
			fmt.Printf("Or download from: %s\n", info.ReleaseURL)
			fmt.Println()

			if info.IsPrerelease {
				fmt.Println("⚠️  This is a pre-release version.")
				fmt.Println()
			}
		} else {
			fmt.Printf("✓ You're running the latest version (%s)\n", info.CurrentVersion)
		}
	}
	return nil
}

// buildGitHubAPIURL constructs the GitHub API URL based on binary info
func buildGitHubAPIURL() string {
	repo := GitHubRepo
	if repo == "" {
		repo = "cimis-cli"
	}
	return fmt.Sprintf("https://api.github.com/repos/dl-alexandre/%s/releases/latest", repo)
}

// normalizeVersion ensures version starts with 'v'
func normalizeVersion(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return "v0.0.0"
	}
	if !strings.HasPrefix(v, "v") && !strings.HasPrefix(v, "V") {
		return "v" + v
	}
	return strings.ToLower(v)
}

// compareVersions compares two semantic versions
// Returns -1 if v1 < v2, 0 if v1 == v2, 1 if v1 > v2
func compareVersions(v1, v2 string) int {
	// Remove 'v' prefix
	v1 = strings.TrimPrefix(v1, "v")
	v2 = strings.TrimPrefix(v2, "v")

	// Split into parts
	parts1 := strings.Split(v1, ".")
	parts2 := strings.Split(v2, ".")

	// Compare each part
	maxLen := len(parts1)
	if len(parts2) > maxLen {
		maxLen = len(parts2)
	}

	for i := 0; i < maxLen; i++ {
		var num1, num2 int

		if i < len(parts1) {
			// Extract numeric part before any pre-release identifier
			part := parts1[i]
			if idx := strings.IndexAny(part, "-"); idx != -1 {
				part = part[:idx]
			}
			fmt.Sscanf(part, "%d", &num1)
		}

		if i < len(parts2) {
			part := parts2[i]
			if idx := strings.IndexAny(part, "-"); idx != -1 {
				part = part[:idx]
			}
			fmt.Sscanf(part, "%d", &num2)
		}

		if num1 < num2 {
			return -1
		}
		if num1 > num2 {
			return 1
		}
	}

	// Check pre-release status (v1.0.0-alpha < v1.0.0)
	if strings.Contains(v1, "-") && !strings.Contains(v2, "-") {
		return -1
	}
	if !strings.Contains(v1, "-") && strings.Contains(v2, "-") {
		return 1
	}

	return 0
}

// getCacheDir returns the cache directory path
func getCacheDir() string {
	// Check environment variable first
	if dir := os.Getenv("CIMIS_CACHE_DIR"); dir != "" {
		return dir
	}

	// Use standard cache location
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return ".cache/cimis"
	}

	return filepath.Join(homeDir, ".cache", "cimis")
}

// getCacheFile returns the path to the update check cache file
func getCacheFile() string {
	return filepath.Join(getCacheDir(), "update_check.json")
}

// cacheEntry represents a cached update check
type cacheEntry struct {
	Data      UpdateInfo `json:"data"`
	CreatedAt time.Time  `json:"created_at"`
}

// cacheUpdateInfo saves update info to cache
func cacheUpdateInfo(info UpdateInfo) {
	cacheFile := getCacheFile()

	// Ensure cache directory exists
	if err := os.MkdirAll(filepath.Dir(cacheFile), 0755); err != nil {
		return
	}

	entry := cacheEntry{
		Data:      info,
		CreatedAt: time.Now(),
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return
	}

	_ = os.WriteFile(cacheFile, data, 0644)
}

// getCachedUpdateInfo retrieves cached update info if still valid
func getCachedUpdateInfo() (UpdateInfo, bool) {
	cacheFile := getCacheFile()

	data, err := os.ReadFile(cacheFile)
	if err != nil {
		return UpdateInfo{}, false
	}

	var entry cacheEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return UpdateInfo{}, false
	}

	// Check if expired
	if time.Since(entry.CreatedAt) > cacheTTL {
		_ = os.Remove(cacheFile)
		return UpdateInfo{}, false
	}

	return entry.Data, true
}

// AutoUpdateCheck performs a background update check (for use at startup)
// It returns immediately and doesn't block
func AutoUpdateCheck() {
	// Skip in CI environments
	if isCIEnvironment() {
		return
	}

	// Check if we've already checked recently
	if _, ok := getCachedUpdateInfo(); ok {
		return
	}

	// Perform check in background
	go func() {
		currentVersion := Version
		if currentVersion == "" || currentVersion == "dev" {
			currentVersion = "v0.0.0"
		}

		info, err := fetchLatestRelease(currentVersion)
		if err != nil {
			return // Silently fail on auto-check
		}

		// Cache the result
		cacheUpdateInfo(info)

		// Only print if update is available
		if info.UpdateAvailable {
			fmt.Println()
			fmt.Printf("📦 A new version is available: %s (current: %s)\n", info.LatestVersion, info.CurrentVersion)
			fmt.Printf("   Run '%s check-updates' for details or upgrade with: brew upgrade %s\n", BinaryName, BinaryName)
			fmt.Println()
		}
	}()
}

// isCIEnvironment checks if we're running in a CI environment
func isCIEnvironment() bool {
	ciVars := []string{"CI", "GITHUB_ACTIONS", "GITLAB_CI", "CIRCLECI", "TRAVIS", "JENKINS_URL", "BUILDKITE"}
	for _, v := range ciVars {
		if _, ok := os.LookupEnv(v); ok {
			return true
		}
	}
	return false
}
